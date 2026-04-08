package diff

import (
	"strings"
	"testing"
)

func TestCompare_Match(t *testing.T) {
	actual := []Row{
		{"user_id": int64(10), "total": int64(300)},
		{"user_id": int64(20), "total": int64(50)},
	}
	expected := []Row{
		{"user_id": int64(20), "total": int64(50)},
		{"user_id": int64(10), "total": int64(300)},
	}
	r := Compare(actual, expected)
	if !r.Match {
		t.Errorf("expected match, got diff:\n%s", r.Format())
	}
}

func TestCompare_ExtraRow(t *testing.T) {
	actual := []Row{
		{"id": int64(1)},
		{"id": int64(2)},
		{"id": int64(3)},
	}
	expected := []Row{
		{"id": int64(1)},
		{"id": int64(2)},
	}
	r := Compare(actual, expected)
	if r.Match {
		t.Fatal("expected mismatch")
	}
	if len(r.Extra) != 1 {
		t.Errorf("expected 1 extra, got %d", len(r.Extra))
	}
	if len(r.Missing) != 0 {
		t.Errorf("expected 0 missing, got %d", len(r.Missing))
	}
}

func TestCompare_MissingRow(t *testing.T) {
	actual := []Row{
		{"id": int64(1)},
	}
	expected := []Row{
		{"id": int64(1)},
		{"id": int64(2)},
	}
	r := Compare(actual, expected)
	if r.Match {
		t.Fatal("expected mismatch")
	}
	if len(r.Missing) != 1 {
		t.Errorf("expected 1 missing, got %d", len(r.Missing))
	}
}

func TestCompare_WrongValue(t *testing.T) {
	actual := []Row{
		{"user_id": int64(10), "total": int64(300)},
		{"user_id": int64(20), "total": int64(50)},
	}
	expected := []Row{
		{"user_id": int64(10), "total": int64(999)},
		{"user_id": int64(20), "total": int64(50)},
	}
	r := Compare(actual, expected)
	if r.Match {
		t.Fatal("expected mismatch")
	}
	if len(r.Extra) != 1 || len(r.Missing) != 1 {
		t.Errorf("expected 1 extra + 1 missing, got extra=%d missing=%d", len(r.Extra), len(r.Missing))
	}
}

func TestCompare_DuplicateRows(t *testing.T) {
	actual := []Row{
		{"id": int64(1)},
		{"id": int64(1)},
		{"id": int64(1)},
	}
	expected := []Row{
		{"id": int64(1)},
		{"id": int64(1)},
	}
	r := Compare(actual, expected)
	if r.Match {
		t.Fatal("expected mismatch")
	}
	if len(r.Extra) != 1 {
		t.Errorf("expected 1 extra, got %d", len(r.Extra))
	}
}

func TestFormat_Output(t *testing.T) {
	r := &Result{
		Match: false,
		Extra: []IndexedRow{
			{Row: Row{"user_id": int64(10), "total": int64(300)}, Index: 1},
		},
		Missing: []IndexedRow{
			{Row: Row{"user_id": int64(10), "total": int64(999)}, Index: 1},
		},
		Columns: []string{"total", "user_id"},
	}
	out := r.Format()
	if !strings.Contains(out, "- ") {
		t.Errorf("expected '-' prefix for missing rows:\n%s", out)
	}
	if !strings.Contains(out, "+ ") {
		t.Errorf("expected '+' prefix for extra rows:\n%s", out)
	}
	if !strings.Contains(out, "999") {
		t.Errorf("expected missing value 999:\n%s", out)
	}
	if !strings.Contains(out, "300") {
		t.Errorf("expected extra value 300:\n%s", out)
	}
	if !strings.Contains(out, "expected row 1") {
		t.Errorf("expected row number annotation:\n%s", out)
	}
}
