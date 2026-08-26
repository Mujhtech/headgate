// headgatectl is the bounded, API-only incident CLI. It never connects to a store
// directly, so its authorization and behavior are exactly the control API's.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

type client struct {
	base, token string
	http        *http.Client
}

func (c client) call(method, path string, body any) error {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, strings.TrimRight(c.base, "/")+"/api/v1"+path, r)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	if method != http.MethodGet {
		req.Header.Set("Idempotency-Key", fmt.Sprintf("headgatectl-%d", time.Now().UnixNano()))
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	if len(bytes.TrimSpace(b)) != 0 {
		var pretty bytes.Buffer
		if json.Indent(&pretty, b, "", "  ") == nil {
			b = pretty.Bytes()
		}
		fmt.Println(string(b))
	}
	return nil
}

func main() {
	var base, token string
	c := client{http: &http.Client{Timeout: 30 * time.Second}}
	root := &cobra.Command{Use: "headgatectl", Short: "Operate a Headgate fleet through its bounded control API", SilenceUsage: true, RunE: func(*cobra.Command, []string) error { return errors.New("choose jobs, queues, or operations") }}
	root.PersistentFlags().StringVar(&base, "api", env("HEADGATE_API", "http://127.0.0.1:8080"), "control API origin")
	root.PersistentFlags().StringVar(&token, "token", os.Getenv("HEADGATE_TOKEN"), "bearer token (or HEADGATE_TOKEN)")
	root.PersistentPreRun = func(*cobra.Command, []string) { c.base, c.token = base, token }

	jobs := &cobra.Command{Use: "jobs", Short: "Inspect and control jobs"}
	var limit int
	list := &cobra.Command{Use: "list", RunE: func(*cobra.Command, []string) error {
		return c.call(http.MethodGet, "/jobs?limit="+fmt.Sprint(limit), nil)
	}}
	list.Flags().IntVar(&limit, "limit", 50, "page size (server-capped)")
	show := &cobra.Command{Use: "show ID", Args: cobra.ExactArgs(1), RunE: func(_ *cobra.Command, a []string) error {
		return c.call(http.MethodGet, "/jobs/"+url.PathEscape(a[0]), nil)
	}}
	jobs.AddCommand(list, show)
	for _, action := range []string{"promote", "retry", "cancel"} {
		action := action
		jobs.AddCommand(&cobra.Command{Use: action + " ID", Args: cobra.ExactArgs(1), RunE: func(_ *cobra.Command, a []string) error {
			return c.call(http.MethodPost, "/jobs/"+url.PathEscape(a[0])+"/"+action, map[string]any{})
		}})
	}
	jobs.AddCommand(&cobra.Command{Use: "delete ID", Args: cobra.ExactArgs(1), RunE: func(_ *cobra.Command, a []string) error {
		return c.call(http.MethodDelete, "/jobs/"+url.PathEscape(a[0]), nil)
	}})

	queues := &cobra.Command{Use: "queues", Short: "Inspect and control queues"}
	queues.AddCommand(&cobra.Command{Use: "list", RunE: func(*cobra.Command, []string) error { return c.call(http.MethodGet, "/queues", nil) }})
	var force bool
	delq := &cobra.Command{Use: "delete QUEUE", Args: cobra.ExactArgs(1), RunE: func(_ *cobra.Command, a []string) error {
		return c.call(http.MethodDelete, "/queues/"+url.PathEscape(a[0])+"?force="+fmt.Sprint(force), nil)
	}}
	delq.Flags().BoolVar(&force, "force", false, "start an audited asynchronous delete when non-empty")
	queues.AddCommand(delq, &cobra.Command{Use: "sample-memory", RunE: func(*cobra.Command, []string) error {
		return c.call(http.MethodPost, "/queues/actions/sample-memory", map[string]any{})
	}})

	operations := &cobra.Command{Use: "operations", Short: "Inspect asynchronous operations"}
	operations.AddCommand(&cobra.Command{Use: "show ID", Args: cobra.ExactArgs(1), RunE: func(_ *cobra.Command, a []string) error {
		return c.call(http.MethodGet, "/operations/"+url.PathEscape(a[0]), nil)
	}})
	root.AddCommand(jobs, queues, operations)
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "headgatectl:", err)
		os.Exit(1)
	}
}

func env(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}
