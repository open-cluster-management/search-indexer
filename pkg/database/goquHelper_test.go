// Copyright Contributors to the Open Cluster Management project

package database

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_useGoqu(t *testing.T) {
	q, p, er := useGoqu("SELECT uid FROM search.resources WHERE cluster=$1 AND uid!='cluster__$1'", []interface{}{"test-cluster"})

	assert.Equal(t, "SELECT \"uid\" FROM \"search\".\"resources\" WHERE ((\"cluster\" = $1) AND (\"uid\" != $2))", q)
	assert.Equal(t, []interface{}{"test-cluster", "cluster__test-cluster"}, p)
	assert.Nil(t, er)
}

func Test_useGoqu_invalidParams(t *testing.T) {
	q, p, er := useGoqu("INSERT into search.resources values($1,$2,$3) ON CONFLICT (uid) DO UPDATE SET data=$3 WHERE data!=$3", []interface{}{"fakeUid", "fakeCluster"})

	assert.Equal(t, "", q)
	assert.Nil(t, p)
	assert.NotNil(t, er)
}
