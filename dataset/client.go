package dataset

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

type Client struct {
	client    *bigquery.Client
	projectID string
	location  string
}

func New(ctx context.Context, projectID, location string) (*Client, error) {
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("new bigquery client: %v", err)
	}

	loc := "US"
	if len(location) > 0 {
		loc = location
	}

	return &Client{
		client:    client,
		projectID: projectID,
		location:  loc,
	}, nil
}

func (c *Client) Create(ctx context.Context, dsn string, meta []bigquery.TableMetadata) error {
	if _, err := c.client.Dataset(dsn).Metadata(ctx); err != nil {
		// not found then create dataset
		if err := c.client.Dataset(dsn).Create(ctx, &bigquery.DatasetMetadata{
			Location: c.location,
		}); err != nil {
			return fmt.Errorf("create %v: %v", dsn, err)
		}
	}

	for i := range meta {
		ref := c.client.Dataset(dsn).Table(meta[i].Name)
		if _, err := ref.Metadata(ctx); err == nil {
			// already exists
			continue
		}

		if err := ref.Create(ctx, &meta[i]); err != nil {
			return fmt.Errorf("create %v/%v: %v", dsn, meta[i].Name, err)
		}
	}

	return nil
}

func (c *Client) Delete(ctx context.Context, dsn string, meta []bigquery.TableMetadata) error {
	if _, err := c.client.Dataset(dsn).Metadata(ctx); err != nil {
		return fmt.Errorf("dataset(%v): %v", dsn, err)
	}

	for _, t := range meta {
		ref := c.client.Dataset(dsn).Table(t.Name)
		if _, err := ref.Metadata(ctx); err != nil {
			// https://pkg.go.dev/cloud.google.com/go/bigquery#hdr-Errors
			var e *googleapi.Error
			if ok := errors.As(err, &e); ok && e.Code == http.StatusNotFound {
				// already deleted
				return nil
			}

			return fmt.Errorf("table(%v): %v", t.Name, err)
		}

		if err := ref.Delete(ctx); err != nil {
			return fmt.Errorf("delete table=%v: %v", t.Name, err)
		}
	}

	return nil
}

func (c *Client) Tables(ctx context.Context, dsn string) ([]bigquery.TableMetadata, error) {
	if _, err := c.client.Dataset(dsn).Metadata(ctx); err != nil {
		return nil, fmt.Errorf("dataset(%v): %v", dsn, err)
	}

	tables := make([]bigquery.TableMetadata, 0)
	it := c.client.Dataset(dsn).Tables(ctx)
	for {
		t, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("table: %v", err)
		}

		m, err := t.Metadata(ctx)
		if err != nil {
			return nil, fmt.Errorf("table metadata: %v", err)
		}

		tables = append(tables, *m)
	}

	return tables, nil
}

func (c *Client) Insert(ctx context.Context, dsn, table string, items []interface{}) error {
	if err := c.client.Dataset(dsn).Table(table).Inserter().Put(ctx, items); err != nil {
		return fmt.Errorf("insert %v.%v.%v: %v", c.projectID, dsn, table, err)
	}

	return nil
}

func (c *Client) Query(ctx context.Context, query string, fn func(values []bigquery.Value)) error {
	q := c.client.Query(query)
	q.Location = c.location

	it, err := q.Read(ctx)
	if err != nil {
		return fmt.Errorf("query: %v", err)
	}

	for {
		var values []bigquery.Value
		err := it.Next(&values)
		if err == iterator.Done {
			break
		}

		if err != nil {
			return fmt.Errorf("iterator: %v", err)
		}

		if len(values) == 0 {
			continue
		}

		nilv := false
		for _, v := range values {
			if v == nil {
				nilv = true
				break
			}
		}

		if nilv {
			continue
		}

		fn(values)
	}

	return nil
}

func (c *Client) Close() error {
	return c.client.Close()
}

func Tables(ctx context.Context, projectID, location, dsn string) ([]bigquery.TableMetadata, error) {
	c, err := New(ctx, projectID, location)
	if err != nil {
		return nil, fmt.Errorf("new client: %v", err)
	}
	defer c.Close()

	return c.Tables(ctx, dsn)
}

func Create(ctx context.Context, projectID, location, dsn string, meta []bigquery.TableMetadata) error {
	c, err := New(ctx, projectID, location)
	if err != nil {
		return fmt.Errorf("new client: %v", err)
	}
	defer c.Close()

	return c.Create(ctx, dsn, meta)
}

func Delete(ctx context.Context, projectID, location, dsn string, meta []bigquery.TableMetadata) error {
	c, err := New(ctx, projectID, location)
	if err != nil {
		return fmt.Errorf("new client: %v", err)
	}
	defer c.Close()

	return c.Delete(ctx, dsn, meta)
}

func Insert(ctx context.Context, projectID, location, dsn, table string, items []interface{}) error {
	c, err := New(ctx, projectID, location)
	if err != nil {
		return fmt.Errorf("new client: %v", err)
	}
	defer c.Close()

	return c.Insert(ctx, dsn, table, items)
}

func Query(ctx context.Context, projectID, location, query string, fn func(values []bigquery.Value)) error {
	c, err := New(ctx, projectID, location)
	if err != nil {
		return fmt.Errorf("new client: %v", err)
	}
	defer c.Close()

	return c.Query(ctx, query, fn)
}
