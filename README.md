# README - DapperRepository

Repositório genérico para acesso a dados usando **Dapper**, com foco em simplicidade, performance e baixo acoplamento.
Segue o padrão de Repositório e Unidade de Trabalho descritos por Martin Fowler, sem impor novas linguagens, controle de estado ou proxies dinâmicos.

---

## Instalação

```bash
dotnet add package Rochas.DapperRepository
```

---

## Interface Principal

```csharp
IGenericRepository<T>
```

## Exemplo de uso típico — Instância manual:

```csharp
var connString = "Data Source=sample.db;Cache=Shared";
using var repo = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString);
var result = await repo.Query(new SampleEntity());
```

## Exemplo de Registro no DI

```csharp
services.AddScoped<IGenericRepository<SampleEntity>>(provider =>
    new GenericRepository<SampleEntity>(
        DatabaseEngine.SQLite,
        configuration.GetConnectionString("Default")));
```

---

## CRUD

### Add
```csharp
var entity = new SampleEntity { Name = "Renato" };
await repo.Add(entity);
```

### AddRange
```csharp
await repo.AddRange(list);
```

### Update
```csharp
int affected = await repo.Update(entityToUpdate, filter);
```

### Remove
```csharp
int removed = await repo.Remove(filterToRemove);
```

---

## PaginatedResult\<T\>

```csharp
public class PaginatedResult<T>
{
    public IReadOnlyList<T> Items { get; set; }
    public int TotalCount { get; set; }
    public int Page { get; set; }
    public int PageSize { get; set; }
    public int PageCount => PageSize > 0 ? (int)Math.Ceiling((double)TotalCount / PageSize) : 0;
}
```

---

## Query Paginado

### Direto
```csharp
var result = await repo.Query(filter, page: 1, pageSize: 20);
var result = repo.QuerySync(filter, page: 1, pageSize: 20);
```

### Via Builder
```csharp
var result = await repo.Query(filter).OrderBy(["Name"]).Paginate(1, 10);
var result = repo.QuerySync(filter).OrderBy(["Name"]).Paginate(1, 10);
```

---

## Query (consultas por filtro tipado)

### Builder assíncrono
```csharp
var result = await repo.Query(filter).OrderBy(["Name"]);
var result = await repo.Query(filter).OrderByDescending(["Name"]);
var result = await repo.Query(filter).OrderBy(["Age", "Name"]);
```

### Builder síncrono
```csharp
var list = repo.QuerySync(filter).OrderBy(["Name"]).ToList();
var list = repo.QuerySync(filter).OrderByDescending(["Name"]).ToList();
```

### OrderBy no repositório
```csharp
var result = await repo.OrderBy(["Name"]);
var result = await repo.OrderByDescending(["Name"]);
```

---

## Search Paginado

### Direto
```csharp
var result = await repo.Search("Paulo", page: 1, pageSize: 20);
```

---

## Search (buscas usando `[Filterable]`)

### Builder assíncrono
```csharp
var result = await repo.Search("Paulo").OrderBy(["Name"]);
var result = await repo.Search("Paulo").OrderByDescending(["Name"]);
```

### Builder síncrono
```csharp
var list = repo.SearchSync("Paulo").OrderBy(["Name"]).ToList();
```

### Count via builder
```csharp
var count = repo.SearchSync("Paulo").Count();
```

---

## GroupBy (agrupamento com agregações)

### Builder assíncrono
```csharp
var result = await repo.Query(filter).GroupBy(["Category"]);
```

### GroupBy com agregações
```csharp
var agg = new Dictionary<string, DataAggregationType>
{
    { "Price", DataAggregationType.Sum },
    { "Price", DataAggregationType.Average }
};
var result = await repo.Query(filter).GroupBy(["Category"], agg);
```

### GroupBy no repositório
```csharp
var result = await repo.GroupBy(["Category"], agg);
```

### GroupBy + OrderBy (encadeamento)
```csharp
var result = await repo.Query(filter)
    .GroupBy(["Category"])
    .OrderByDescending(["Price"]);
```

### Tipos de agregação
| Tipo | Função SQL |
|------|------------|
| `Sum` | `SUM(col)` |
| `Count` | `COUNT(col)` |
| `Minimum` | `MIN(col)` |
| `Maximum` | `MAX(col)` |
| `Average` | `AVG(col)` |

---

## QueryRaw

```csharp
var result = await repo.QueryRaw("SELECT * FROM MyTable WHERE Active = 1", new Dictionary<string, object>());
var result = repo.QueryRawSync("SELECT * FROM MyTable", new Dictionary<string, object>());

// Paginado
var result = await repo.QueryRaw(
    "SELECT * FROM MyTable ORDER BY Name LIMIT 10 OFFSET 0",
    "SELECT COUNT(*) FROM MyTable",
    new Dictionary<string, object>(),
    page: 1, pageSize: 10);
```

---

## Cache

```csharp
[Cacheable]
public class SampleEntity { ... }
var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString, useCache: true);
DataCache.Initialize(memorySizeLimit: 100);
```

---

## Benchmark — DapperRepository vs EF Core

**Windows 11, Intel i5-7500T, .NET 9.0, SQLite, 5.000 rows**

| Cenário | EF Core | DapperRepository | Vitória |
|---------|---------|------------------|---------|
| InsertIndividual | 3.3 ms | 3.1 ms | ORM 1.1x |
| GetById | 558 μs | 507 μs | ORM 1.1x |
| Search_Filterable | 1.0 ms | 1.2 ms | Empate |
| Sort_5000_rows_ORDER_BY | 62 ms | 39 ms | ORM 1.6x |
| Sort_MultiColumn | 61 ms | 41 ms | ORM 1.5x |
| GroupBy_Simple | 7.3 ms | 1.0 ms | ORM 7.3x |
| GroupBy_AggAll | 3.1 ms | 1.0 ms | ORM 3.1x |
| GroupBy_Having | 2.5 ms | 1.1 ms | ORM 2.3x |
| CountSync | 504 μs | 870 μs | EF 1.7x |
| Update_Individual | 2.9 ms | 3.9 ms | EF 1.3x |
| QueryRaw_Select | 3.5 ms | 3.2 ms | ORM 1.1x |
