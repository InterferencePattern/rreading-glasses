package internal

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestInstrument(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()

	notFoundGetter := NewMockgetter(gomock.NewController(t))
	notFoundGetter.EXPECT().GetAuthor(gomock.Any(), int64(123)).Return(nil, errNotFound).AnyTimes()

	ctrl, err := NewController(newMemoryCache(), notFoundGetter, nil, reg)
	require.NoError(t, err)

	h := NewHandler(ctrl)
	mux := NewMux(h, reg)

	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	resp, err := http.Get(ts.URL + "/author/123")
	assert.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	resp, err = http.Get(ts.URL + "/debug/metrics")
	assert.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	assert.Contains(t, string(got), `http_inflight 1`)
	assert.Contains(t, string(got), `http_requests_bucket{method="GET",path="/author",status="404",le="0.001"} 1`)
}

func TestControllerMetrics(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()

	cm := newControllerMetrics(reg)

	// Simulate denorm flow
	cm.denormWaitingSet(1)

	// Simulate refresh flow
	cm.refreshWaitingAdd(3)
	cm.refreshWaitingAdd(-3)

	// ETag matches/mismatches
	cm.etagMatchesInc()
	cm.etagMismatchesInc()

	assert.Equal(t, 1.0, cm.denormWaitingGet())
	assert.Equal(t, 0.0, cm.refreshWaitingGet())
	assert.Equal(t, 1.0, cm.etagMatchesGet())
	assert.Equal(t, 1.0, cm.etagMismatchesGet())
}

func TestCacheMetrics(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	cm := newCacheMetrics(reg)

	cm.cacheHitInc()
	cm.cacheMissInc()

	assert.Equal(t, int64(1), cm.cacheHitGet())
	assert.Equal(t, int64(1), cm.cacheMissGet())
	assert.Equal(t, 0.5, cm.cacheHitRatioGet())
}

func TestNormalizePattern(t *testing.T) {
	assert.Equal(t, "/author", normalizePattern("/author/{foreignAuthorID}"))
	assert.Equal(t, "/book/bulk", normalizePattern("/book/bulk/"))
}

// statsTx returns a transaction in which "cache" is an empty temporary table,
// so stats can be tested without interference from other tests.
func statsTx(t *testing.T) pgx.Tx {
	t.Helper()

	db, err := newDB(t.Context(), "postgres://postgres@localhost:5432/test")
	require.NoError(t, err)
	t.Cleanup(db.Close)

	tx, err := db.Begin(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })

	_, err = tx.Exec(t.Context(), `CREATE TEMP TABLE cache (LIKE public.cache INCLUDING DEFAULTS) ON COMMIT DROP;`)
	require.NoError(t, err)

	return tx
}

func TestCollectDBStats(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		tx := statsTx(t)

		stats, err := collectDBStats(t.Context(), tx, _dbStatsSampleBytes)
		require.NoError(t, err)
		assert.Equal(t, dbStats{}, stats)
	})

	t.Run("small tables are exact", func(t *testing.T) {
		tx := statsTx(t)

		for _, key := range []string{"a1", "a2", "a3", "b1", "b2", "w1", "ra1", "s1", "z1", "i1", "i2"} {
			_, err := tx.Exec(t.Context(), `INSERT INTO cache (key, value) VALUES ($1, '');`, key)
			require.NoError(t, err)
		}

		stats, err := collectDBStats(t.Context(), tx, _dbStatsSampleBytes)
		require.NoError(t, err)
		assert.Equal(t, dbStats{
			authors:    3,
			editions:   2,
			works:      1,
			refreshing: 1,
			series:     1,
			asin:       1,
			isbn:       2,
		}, stats)
	})

	t.Run("large tables are estimated", func(t *testing.T) {
		tx := statsTx(t)

		// 10k each of authors and works, and 20k editions, interleaved.
		_, err := tx.Exec(t.Context(), `
		  INSERT INTO cache (key, value)
		  SELECT (ARRAY['a', 'b', 'w', 'b'])[1 + n % 4] || n, decode(repeat('00', 200), 'hex')
		  FROM generate_series(1, 40000) AS n;
		`)
		require.NoError(t, err)

		var size int64
		err = tx.QueryRow(t.Context(), `SELECT pg_relation_size('cache');`).Scan(&size)
		require.NoError(t, err)

		// Only read a quarter of the table.
		stats, err := collectDBStats(t.Context(), tx, size/4)
		require.NoError(t, err)

		assert.InEpsilon(t, 10000, stats.authors, 0.4)
		assert.InEpsilon(t, 20000, stats.editions, 0.4)
		assert.InEpsilon(t, 10000, stats.works, 0.4)
		assert.Zero(t, stats.series)
	})
}
