// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
package grainlift

// Large requests and results through S3-compatible object storage: VGI-RPC
// external locations over HTTP.
//
// A client whose request exceeds the HTTP request limit asks for an upload URL
// (POST /__upload_url__/init), PUTs the request to the bucket and sends only a
// pointer; a result batch at or over the threshold is stored in the bucket and
// the client is sent a URL to fetch it. Both URLs are presigned here (AWS
// Signature Version 4, query-string authentication), so clients need no
// storage credentials and no AWS SDK is linked.

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// ExternalStorageConfig names an S3-compatible bucket (AWS S3, Cloudflare R2,
// MinIO, ...) for large HTTP requests and results. Objects are never deleted
// by the service; give the bucket a lifecycle rule that expires them. Browser
// clients need a CORS rule on the bucket allowing PUT and GET from their
// origin.
//
// Formatting the config (fmt, %v, %+v, %#v) never prints the secret key.
type ExternalStorageConfig struct {
	// Endpoint is the S3 API endpoint, e.g.
	// https://<account>.r2.cloudflarestorage.com or
	// https://s3.us-east-1.amazonaws.com.
	Endpoint string
	Bucket   string
	// Region is the signing region; empty means "auto" (R2).
	Region string
	// Prefix is prepended to every object key.
	Prefix string
	// AccessKeyID and SecretAccessKey default to the AWS_ACCESS_KEY_ID and
	// AWS_SECRET_ACCESS_KEY environment variables.
	AccessKeyID     string
	SecretAccessKey string `json:"-"`
	// VirtualHostedStyle addresses objects as https://<bucket>.<endpoint
	// host>/<key> instead of <endpoint>/<bucket>/<key>.
	VirtualHostedStyle bool
	// URLTTL is how long presigned URLs stay valid: 1s to 7 days; zero means
	// 15 minutes.
	URLTTL time.Duration
	// ThresholdBytes is the result batch size at which batches go to the
	// bucket; zero means 1 MiB.
	ThresholdBytes int64
	// MaxUploadBytes is the largest request a client may upload (advertised,
	// and enforced when the service fetches it); zero means 256 MiB.
	MaxUploadBytes int64
	// HTTPClient carries the service's own PUTs and GETs to the bucket; nil
	// means a client with a 5 minute timeout that does not follow redirects.
	HTTPClient *http.Client
}

const (
	defaultStorageRegion   = "auto"
	defaultStorageURLTTL   = 15 * time.Minute
	maxStorageURLTTL       = 7 * 24 * time.Hour // SigV4 presigned URL limit
	defaultStorageMinBytes = 1 << 20
	defaultStorageMaxBytes = 256 << 20
)

// Format prints the config with the secret key redacted.
func (c ExternalStorageConfig) Format(f fmt.State, verb rune) {
	secret := ""
	if c.SecretAccessKey != "" {
		secret = "<redacted>"
	}
	fmt.Fprintf(f, "ExternalStorageConfig{Endpoint:%q Bucket:%q Region:%q Prefix:%q AccessKeyID:%q SecretAccessKey:%q VirtualHostedStyle:%t URLTTL:%s ThresholdBytes:%d MaxUploadBytes:%d}",
		c.Endpoint, c.Bucket, c.Region, c.Prefix, c.AccessKeyID, secret, c.VirtualHostedStyle, c.URLTTL, c.ThresholdBytes, c.MaxUploadBytes)
}

// String returns the config with the secret key redacted.
func (c ExternalStorageConfig) String() string { return fmt.Sprint(c) }

// GoString returns the config with the secret key redacted.
func (c ExternalStorageConfig) GoString() string { return fmt.Sprint(c) }

// resolved applies defaults and the environment credentials, and validates.
func (c ExternalStorageConfig) resolved() (ExternalStorageConfig, error) {
	if c.Region == "" {
		c.Region = defaultStorageRegion
	}
	if c.URLTTL == 0 {
		c.URLTTL = defaultStorageURLTTL
	}
	if c.ThresholdBytes == 0 {
		c.ThresholdBytes = defaultStorageMinBytes
	}
	if c.MaxUploadBytes == 0 {
		c.MaxUploadBytes = defaultStorageMaxBytes
	}
	if c.AccessKeyID == "" {
		c.AccessKeyID = os.Getenv("AWS_ACCESS_KEY_ID")
	}
	if c.SecretAccessKey == "" {
		c.SecretAccessKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
	}
	endpoint, e := url.Parse(c.Endpoint)
	switch {
	case e != nil || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.Host == "":
		return c, errors.New("external storage endpoint must be an absolute http(s) URL")
	case endpoint.RawQuery != "" || endpoint.Fragment != "" || endpoint.User != nil:
		return c, errors.New("external storage endpoint must not have credentials, a query or a fragment")
	case strings.TrimSpace(c.Bucket) == "":
		return c, errors.New("external storage bucket must not be empty")
	case strings.TrimSpace(c.Region) == "":
		return c, errors.New("external storage region must not be empty")
	case c.URLTTL < time.Second || c.URLTTL > maxStorageURLTTL:
		return c, errors.New("external storage URL TTL must be between 1s and 7 days")
	case c.ThresholdBytes <= 0 || c.MaxUploadBytes <= 0:
		return c, errors.New("external storage threshold and upload limit must be positive")
	case c.AccessKeyID == "" || c.SecretAccessKey == "":
		return c, errors.New("external storage needs credentials: set them in the config or AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
	}
	return c, nil
}

// presigner signs S3 object URLs with AWS Signature Version 4 (query string).
type presigner struct {
	base                 *url.URL // ends in "/"; objects are base + key
	region               string
	accessKey, secretKey string
}

func newPresigner(endpoint, bucket, region, accessKey, secretKey string, virtualHosted bool) (*presigner, error) {
	base, e := url.Parse(endpoint)
	if e != nil {
		return nil, e
	}
	path := strings.TrimRight(base.Path, "/")
	if virtualHosted {
		base.Host = bucket + "." + base.Host
		base.Path = path + "/"
	} else {
		base.Path = path + "/" + bucket + "/"
	}
	base.RawPath = ""
	base.RawQuery, base.Fragment = "", ""
	return &presigner{base: base, region: region, accessKey: accessKey, secretKey: secretKey}, nil
}

// prefix is the escaped object path prefix every URL of this bucket shares.
func (p *presigner) prefix() string {
	segments := strings.Split(strings.TrimSuffix(p.base.Path, "/"), "/")
	for i, s := range segments {
		segments[i] = uriEncode(s, true)
	}
	return strings.Join(segments, "/") + "/"
}

// presign returns a URL valid for method on key for expires from now.
func (p *presigner) presign(method, key string, now time.Time, expires time.Duration) string {
	path := p.prefix() + uriEncode(key, false)
	host := p.base.Host
	now = now.UTC()
	date := now.Format("20060102")
	timestamp := now.Format("20060102T150405Z")
	scope := date + "/" + p.region + "/s3/aws4_request"
	params := map[string]string{
		"X-Amz-Algorithm":     "AWS4-HMAC-SHA256",
		"X-Amz-Credential":    p.accessKey + "/" + scope,
		"X-Amz-Date":          timestamp,
		"X-Amz-Expires":       strconv.FormatInt(int64(expires/time.Second), 10),
		"X-Amz-SignedHeaders": "host",
	}
	query := make([]string, 0, len(params))
	for k, v := range params {
		query = append(query, uriEncode(k, true)+"="+uriEncode(v, true))
	}
	sort.Strings(query)
	canonicalQuery := strings.Join(query, "&")
	canonical := method + "\n" + path + "\n" + canonicalQuery + "\nhost:" + host + "\n\nhost\nUNSIGNED-PAYLOAD"
	digest := sha256.Sum256([]byte(canonical))
	toSign := "AWS4-HMAC-SHA256\n" + timestamp + "\n" + scope + "\n" + hex.EncodeToString(digest[:])
	signingKey := hmacSHA256([]byte("AWS4"+p.secretKey), date)
	for _, part := range []string{p.region, "s3", "aws4_request"} {
		signingKey = hmacSHA256(signingKey, part)
	}
	signature := hex.EncodeToString(hmacSHA256(signingKey, toSign))
	return p.base.Scheme + "://" + host + path + "?" + canonicalQuery + "&X-Amz-Signature=" + signature
}

// validate accepts only this bucket's objects: the service fetches nothing
// else, so a client cannot point it at an internal address.
func (p *presigner) validate(raw string) error {
	u, e := url.Parse(raw)
	if e != nil {
		return errors.New("invalid external location URL")
	}
	if u.Scheme != p.base.Scheme || u.Host != p.base.Host || !strings.HasPrefix(u.EscapedPath(), p.prefix()) {
		return errors.New("external location URL is not in this service's storage bucket")
	}
	return nil
}

func hmacSHA256(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(data))
	return mac.Sum(nil)
}

// uriEncode is SigV4 URI encoding: everything but unreserved characters, and
// "/" too unless it separates path segments.
func uriEncode(value string, encodeSlash bool) string {
	var b strings.Builder
	for i := 0; i < len(value); i++ {
		c := value[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-', c == '_', c == '.', c == '~':
			b.WriteByte(c)
		case c == '/' && !encodeSlash:
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// s3Storage stores result batches and vends upload URLs in one bucket.
type s3Storage struct {
	signer *presigner
	prefix string
	ttl    time.Duration
	client *http.Client
}

func (s *s3Storage) pair() (vgirpc.UploadURL, error) {
	var id [16]byte
	if _, e := rand.Read(id[:]); e != nil {
		return vgirpc.UploadURL{}, e
	}
	id[6] = id[6]&0x0f | 0x40
	id[8] = id[8]&0x3f | 0x80
	h := hex.EncodeToString(id[:])
	key := s.prefix + h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:] + ".arrow"
	now := time.Now()
	return vgirpc.UploadURL{
		UploadURL:   s.signer.presign(http.MethodPut, key, now, s.ttl),
		DownloadURL: s.signer.presign(http.MethodGet, key, now, s.ttl),
		ExpiresAt:   now.Add(s.ttl).UTC(),
	}, nil
}

// GenerateUploadURL implements vgirpc.UploadURLProvider.
func (s *s3Storage) GenerateUploadURL(*arrow.Schema) (vgirpc.UploadURL, error) { return s.pair() }

// Upload implements vgirpc.ExternalStorage: PUT data, return a GET URL.
func (s *s3Storage) Upload(data []byte, _ *arrow.Schema, contentEncoding string) (string, error) {
	urls, e := s.pair()
	if e != nil {
		return "", e
	}
	req, e := http.NewRequest(http.MethodPut, urls.UploadURL, bytes.NewReader(data))
	if e != nil {
		return "", e
	}
	req.Header.Set("Content-Type", "application/vnd.apache.arrow.stream")
	if contentEncoding != "" {
		req.Header.Set("Content-Encoding", contentEncoding)
	}
	resp, e := s.client.Do(req)
	if e != nil {
		return "", errors.New("external storage PUT failed")
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	_ = resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return "", fmt.Errorf("external storage PUT returned %d", resp.StatusCode)
	}
	return urls.DownloadURL, nil
}

// externalStorage is the resolved storage wiring for the HTTP handler.
type externalStorage struct {
	location       *vgirpc.ExternalLocationConfig
	uploads        vgirpc.UploadURLProvider
	maxUploadBytes int64
}

func newExternalStorage(config ExternalStorageConfig) (*externalStorage, error) {
	c, e := config.resolved()
	if e != nil {
		return nil, failure("invalid_arguments", e.Error())
	}
	signer, e := newPresigner(c.Endpoint, c.Bucket, c.Region, c.AccessKeyID, c.SecretAccessKey, c.VirtualHostedStyle)
	if e != nil {
		return nil, failure("invalid_arguments", "Invalid external storage endpoint")
	}
	client := c.HTTPClient
	if client == nil {
		client = &http.Client{
			Timeout:       5 * time.Minute,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}
	}
	storage := &s3Storage{signer: signer, prefix: c.Prefix, ttl: c.URLTTL, client: client}
	location := vgirpc.DefaultExternalLocationConfig(storage)
	location.ExternalizeThresholdBytes = c.ThresholdBytes
	location.URLValidator = signer.validate
	location.HTTPClient = client
	// A client upload is fetched whole before it is decoded.
	location.MaxFetchBytes = c.MaxUploadBytes
	location.MaxDecompressedBytes = c.MaxUploadBytes
	return &externalStorage{location: location, uploads: storage, maxUploadBytes: c.MaxUploadBytes}, nil
}
