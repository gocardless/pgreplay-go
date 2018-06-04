# pgreplay-go

## Types of Log

### Simple

This is a basic query that is executed directly against Postgres with no
other steps.

```
2010-12-31 10:59:52.243 UTC|postgres|postgres|4d1db7a8.4227|LOG:  statement: set client_encoding to 'LATIN9'
```

### Prepared statement

```
2010-12-31 10:59:57.870 UTC|postgres|postgres|4d1db7a8.4227|LOG:  execute einf"ug: INSERT INTO runtest (id, c, t, b) VALUES ($1, $2, $3, $4)
2010-12-31 10:59:57.870 UTC|postgres|postgres|4d1db7a8.4227|DETAIL:  parameters: $1 = '6', $2 = 'mit    Tabulator', $3 = '2050-03-31 22:00:00+00', $4 = NULL
```
