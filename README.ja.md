# bqtest

[![Go](https://img.shields.io/badge/Go-1.21+-00ADD8?logo=go)](https://go.dev)

BigQuery SQL テストランナー。SQL 内のテーブル参照をテスト用 fixture に差し替え、BigQuery 上で実行し、結果を期待値と比較します。

**[English](./README.md)**

## 特徴

- **AST ベースのテーブル参照解析** - go-zetasql (Google ZetaSQL) による正確な BigQuery SQL パース
- **宣言的なテスト定義** - YAML でテスト対象 SQL、fixture、期待値を記述
- **BigQuery 実エンジンで実行** - エミュレーションではなく本物の BigQuery でテスト
- **わかりやすい diff 出力** - 失敗時に want vs got を行番号付きで表示
- **CI 対応** - 成功時 exit 0、失敗時 exit 1

## インストール

### バイナリダウンロード

```bash
# macOS (Apple Silicon)
curl -sL https://github.com/matsuri-tech/bqtest/releases/latest/download/bqtest-darwin-arm64.tar.gz | tar xz
mkdir -p ~/.local/bin && mv bqtest-darwin-arm64 ~/.local/bin/bqtest

# Linux (amd64)
curl -sL https://github.com/matsuri-tech/bqtest/releases/latest/download/bqtest-linux-amd64.tar.gz | tar xz
mkdir -p ~/.local/bin && mv bqtest-linux-amd64 ~/.local/bin/bqtest
```

`~/.local/bin` が `PATH` に含まれていることを確認してください。

### ソースから

```bash
go install github.com/matsuri-tech/bqtest/cmd/bqtest@latest
```

## クイックスタート

### 1. テスト対象の SQL を用意

```sql
-- queries/total_amount.sql
SELECT
  user_id,
  SUM(amount) AS total_amount
FROM `myproj.dataset.orders`
GROUP BY user_id
```

### 2. テストケースを YAML で定義

```yaml
# tests/total_amount_test.yaml
test_name: total_amount
sql_file: ../queries/total_amount.sql
fixtures:
  - table: myproj.dataset.orders
    rows:
      - {order_id: 1, user_id: 10, amount: 100}
      - {order_id: 2, user_id: 10, amount: 200}
      - {order_id: 3, user_id: 20, amount: 50}
expected:
  rows:
    - {user_id: 10, total_amount: 300}
    - {user_id: 20, total_amount: 50}
```

### 3. テスト実行

```bash
bqtest tests/total_amount_test.yaml
```

```
PASS  total_amount (2 rows)
  job: abc123def456
```

## 使い方

```bash
bqtest <testfile>...                     # テスト実行
bqtest tests/*.yaml                      # glob パターンで複数実行
bqtest --dry-run tests/test.yaml         # BQ 実行なしで解析結果を表示
bqtest --debug tests/test.yaml           # rewrite 後 SQL と生成スクリプトを表示
bqtest --project my-proj tests/*.yaml    # プロジェクト指定
```

### オプション

| オプション | 説明 |
|---|---|
| `--project <id>` | BigQuery プロジェクト ID（デフォルト: `BQTEST_PROJECT` 環境変数 or `gcloud config`） |
| `--location <loc>` | BigQuery ロケーション（デフォルト: `BQTEST_LOCATION` 環境変数） |
| `--dry-run` | BQ 実行なしでテスト定義の解析結果を表示 |
| `--debug` | rewrite 後の SQL と生成される BigQuery スクリプトを表示 |
| `--keep-script` | 生成スクリプトを `<test_name>.bqtest.sql` に保存 |

## YAML テストケース形式

```yaml
test_name: my_test                       # 必須: テスト名
description: テストの説明                  # 任意
tags: [regression, billing]              # 任意

# テスト対象 SQL（どちらか一方を指定）
sql_file: path/to/query.sql             # SQL ファイルパス（YAML からの相対パス）
sql: "SELECT * FROM `proj.ds.table`"    # またはインライン SQL

# fixture: テーブル参照を差し替えるテストデータ
fixtures:
  - table: myproj.dataset.orders         # 差し替え対象の fully-qualified テーブル名
    rows:                                # テストデータ
      - {order_id: 1, user_id: 10, amount: 100}
      - {order_id: 2, user_id: 10, amount: 200}

  # 複雑な型 (STRUCT, ARRAY) には SQL fixture を使用
  - table: myproj.dataset.events
    sql: "SELECT 1 AS id, STRUCT('a' AS key, 1 AS val) AS metadata"

# 期待される出力
expected:
  rows:
    - {user_id: 10, total_amount: 300}

# 任意: fixture なしで本番テーブルを直接参照するテーブル
passthrough:
  - myproj.dataset.master_data
```

## 失敗時の出力

```
FAIL  my_test
  actual:   2 rows
  expected: 2 rows
  extra:    1 rows (in actual, not in expected)
  missing:  1 rows (in expected, not in actual)

  total_amount  user_id
- 999           10       (expected row 1)
+ 300           10       (actual row 1)
  job: abc123def456
```

- `-` : expected にあるべきだが actual にない行
- `+` : actual にあるが expected にない行

## 動作の仕組み

1. SQL を BigQuery 方言として AST 解析し、テーブル参照を抽出
2. fixture が定義されたテーブルを TEMP TABLE 名に書き換え
3. BigQuery スクリプトを生成（`CREATE TEMP TABLE` + rewrite 後クエリ）
4. BigQuery API で実行し、結果行を取得
5. Go 側で actual と expected の diff を計算・表示

## 前提条件

- BigQuery の実行権限を持つ GCP 認証情報
- `gcloud auth application-default login` または サービスアカウント
- テスト対象 SQL で fully-qualified テーブル名を使用

## ライセンス

MIT
