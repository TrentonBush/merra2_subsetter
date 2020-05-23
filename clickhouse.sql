create table test.round_d_lz
(
    lat Float32 Codec(Delta, LZ4),
    lon Float32 Codec(Delta, LZ4),
    time DateTime('UTC') Codec(Delta, LZ4),
    GHLAND Float32 Codec(Delta, LZ4),
    RHOA Float32 Codec(Delta, LZ4),
    PRECTOTCORR Float32 Codec(Delta, LZ4),
    RISFC Float32 Codec(Delta, LZ4),
    PS Int32 Codec(Delta, LZ4),
    TS Float32 Codec(Delta, LZ4),
    T10M Float32 Codec(Delta, LZ4),
    WS50M Float32 Codec(Delta, LZ4),
    WDIR50M Float32 Codec(Delta, LZ4)
) Engine = MergeTree()
primary key (lat, lon)
order by (lat, lon, time);

create table test.fp16_d_lz
(
    lat Float32 Codec(Delta, LZ4),
    lon Float32 Codec(Delta, LZ4),
    time DateTime('UTC') Codec(Delta, LZ4),
    GHLAND Float32 Codec(Delta, LZ4),
    RHOA Float32 Codec(Delta, LZ4),
    PRECTOTCORR Float32 Codec(Delta, LZ4),
    RISFC Float32 Codec(Delta, LZ4),
    PS Int32 Codec(Delta, LZ4),
    TS Float32 Codec(Delta, LZ4),
    T10M Float32 Codec(Delta, LZ4),
    WS50M Float32 Codec(Delta, LZ4),
    WDIR50M Float32 Codec(Delta, LZ4)
) Engine = MergeTree()
primary key (lat, lon)
order by (lat, lon, time);

create table test.full32_d_lz
(
    lat Float32 Codec(Delta, LZ4),
    lon Float32 Codec(Delta, LZ4),
    time DateTime('UTC') Codec(Delta, LZ4),
    GHLAND Float32 Codec(Delta, LZ4),
    RHOA Float32 Codec(Delta, LZ4),
    PRECTOTCORR Float32 Codec(Delta, LZ4),
    RISFC Float32 Codec(Delta, LZ4),
    PS Int32 Codec(Delta, LZ4),
    TS Float32 Codec(Delta, LZ4),
    T10M Float32 Codec(Delta, LZ4),
    WS50M Float32 Codec(Delta, LZ4),
    WDIR50M Float32 Codec(Delta, LZ4)
) Engine = MergeTree()
primary key (lat, lon)
order by (lat, lon, time);

# -----------------
create table test.round_dd_lz
(
    lat Float32 Codec(DoubleDelta, LZ4),
    lon Float32 Codec(DoubleDelta, LZ4),
    time DateTime('UTC') Codec(DoubleDelta, LZ4),
    GHLAND Float32 Codec(DoubleDelta, LZ4),
    RHOA Float32 Codec(DoubleDelta, LZ4),
    PRECTOTCORR Float32 Codec(DoubleDelta, LZ4),
    RISFC Float32 Codec(DoubleDelta, LZ4),
    PS Int32 Codec(DoubleDelta, LZ4),
    TS Float32 Codec(DoubleDelta, LZ4),
    T10M Float32 Codec(DoubleDelta, LZ4),
    WS50M Float32 Codec(DoubleDelta, LZ4),
    WDIR50M Float32 Codec(DoubleDelta, LZ4)
) Engine = MergeTree()
primary key (lat, lon)
order by (lat, lon, time);

create table test.fp16_dd_lz
(
    lat Float32 Codec(DoubleDelta, LZ4),
    lon Float32 Codec(DoubleDelta, LZ4),
    time DateTime('UTC') Codec(DoubleDelta, LZ4),
    GHLAND Float32 Codec(DoubleDelta, LZ4),
    RHOA Float32 Codec(DoubleDelta, LZ4),
    PRECTOTCORR Float32 Codec(DoubleDelta, LZ4),
    RISFC Float32 Codec(DoubleDelta, LZ4),
    PS Int32 Codec(DoubleDelta, LZ4),
    TS Float32 Codec(DoubleDelta, LZ4),
    T10M Float32 Codec(DoubleDelta, LZ4),
    WS50M Float32 Codec(DoubleDelta, LZ4),
    WDIR50M Float32 Codec(DoubleDelta, LZ4)
) Engine = MergeTree()
primary key (lat, lon)
order by (lat, lon, time);

create table test.full32_dd_lz
(
    lat Float32 Codec(DoubleDelta, LZ4),
    lon Float32 Codec(DoubleDelta, LZ4),
    time DateTime('UTC') Codec(DoubleDelta, LZ4),
    GHLAND Float32 Codec(DoubleDelta, LZ4),
    RHOA Float32 Codec(DoubleDelta, LZ4),
    PRECTOTCORR Float32 Codec(DoubleDelta, LZ4),
    RISFC Float32 Codec(DoubleDelta, LZ4),
    PS Int32 Codec(DoubleDelta, LZ4),
    TS Float32 Codec(DoubleDelta, LZ4),
    T10M Float32 Codec(DoubleDelta, LZ4),
    WS50M Float32 Codec(DoubleDelta, LZ4),
    WDIR50M Float32 Codec(DoubleDelta, LZ4)
) Engine = MergeTree()
primary key (lat, lon)
order by (lat, lon, time);


# ----------------------
create table test.round_gorilla_lz
(
    lat Float32 Codec(Gorilla, LZ4),
    lon Float32 Codec(Gorilla, LZ4),
    time DateTime('UTC') Codec(Gorilla, LZ4),
    GHLAND Float32 Codec(Gorilla, LZ4),
    RHOA Float32 Codec(Gorilla, LZ4),
    PRECTOTCORR Float32 Codec(Gorilla, LZ4),
    RISFC Float32 Codec(Gorilla, LZ4),
    PS Int32 Codec(Gorilla, LZ4),
    TS Float32 Codec(Gorilla, LZ4),
    T10M Float32 Codec(Gorilla, LZ4),
    WS50M Float32 Codec(Gorilla, LZ4),
    WDIR50M Float32 Codec(Gorilla, LZ4)
) Engine = MergeTree()
primary key (lat, lon)
order by (lat, lon, time);

create table test.fp16_gorilla_lz
(
    lat Float32 Codec(Gorilla, LZ4),
    lon Float32 Codec(Gorilla, LZ4),
    time DateTime('UTC') Codec(Gorilla, LZ4),
    GHLAND Float32 Codec(Gorilla, LZ4),
    RHOA Float32 Codec(Gorilla, LZ4),
    PRECTOTCORR Float32 Codec(Gorilla, LZ4),
    RISFC Float32 Codec(Gorilla, LZ4),
    PS Int32 Codec(Gorilla, LZ4),
    TS Float32 Codec(Gorilla, LZ4),
    T10M Float32 Codec(Gorilla, LZ4),
    WS50M Float32 Codec(Gorilla, LZ4),
    WDIR50M Float32 Codec(Gorilla, LZ4)
) Engine = MergeTree()
primary key (lat, lon)
order by (lat, lon, time);

create table test.full32_gorilla_lz
(
    lat Float32 Codec(Gorilla, LZ4),
    lon Float32 Codec(Gorilla, LZ4),
    time DateTime('UTC') Codec(Gorilla, LZ4),
    GHLAND Float32 Codec(Gorilla, LZ4),
    RHOA Float32 Codec(Gorilla, LZ4),
    PRECTOTCORR Float32 Codec(Gorilla, LZ4),
    RISFC Float32 Codec(Gorilla, LZ4),
    PS Int32 Codec(Gorilla, LZ4),
    TS Float32 Codec(Gorilla, LZ4),
    T10M Float32 Codec(Gorilla, LZ4),
    WS50M Float32 Codec(Gorilla, LZ4),
    WDIR50M Float32 Codec(Gorilla, LZ4)
) Engine = MergeTree()
primary key (lat, lon)
order by (lat, lon, time);


# ----------------------
create table test.round_dg_lz
(
    lat Float32 Codec(Delta, Gorilla, LZ4),
    lon Float32 Codec(Delta, Gorilla, LZ4),
    time DateTime('UTC') Codec(Delta, Gorilla, LZ4),
    GHLAND Float32 Codec(Delta, Gorilla, LZ4),
    RHOA Float32 Codec(Delta, Gorilla, LZ4),
    PRECTOTCORR Float32 Codec(Delta, Gorilla, LZ4),
    RISFC Float32 Codec(Delta, Gorilla, LZ4),
    PS Int32 Codec(Delta, Gorilla, LZ4),
    TS Float32 Codec(Delta, Gorilla, LZ4),
    T10M Float32 Codec(Delta, Gorilla, LZ4),
    WS50M Float32 Codec(Delta, Gorilla, LZ4),
    WDIR50M Float32 Codec(Delta, Gorilla, LZ4)
) Engine = MergeTree()
primary key (lat, lon)
order by (lat, lon, time);

create table test.fp16_dg_lz
(
    lat Float32 Codec(Delta, Gorilla, LZ4),
    lon Float32 Codec(Delta, Gorilla, LZ4),
    time DateTime('UTC') Codec(Delta, Gorilla, LZ4),
    GHLAND Float32 Codec(Delta, Gorilla, LZ4),
    RHOA Float32 Codec(Delta, Gorilla, LZ4),
    PRECTOTCORR Float32 Codec(Delta, Gorilla, LZ4),
    RISFC Float32 Codec(Delta, Gorilla, LZ4),
    PS Int32 Codec(Delta, Gorilla, LZ4),
    TS Float32 Codec(Delta, Gorilla, LZ4),
    T10M Float32 Codec(Delta, Gorilla, LZ4),
    WS50M Float32 Codec(Delta, Gorilla, LZ4),
    WDIR50M Float32 Codec(Delta, Gorilla, LZ4)
) Engine = MergeTree()
primary key (lat, lon)
order by (lat, lon, time);

create table test.full32_dg_lz
(
    lat Float32 Codec(Delta, Gorilla, LZ4),
    lon Float32 Codec(Delta, Gorilla, LZ4),
    time DateTime('UTC') Codec(Delta, Gorilla, LZ4),
    GHLAND Float32 Codec(Delta, Gorilla, LZ4),
    RHOA Float32 Codec(Delta, Gorilla, LZ4),
    PRECTOTCORR Float32 Codec(Delta, Gorilla, LZ4),
    RISFC Float32 Codec(Delta, Gorilla, LZ4),
    PS Int32 Codec(Delta, Gorilla, LZ4),
    TS Float32 Codec(Delta, Gorilla, LZ4),
    T10M Float32 Codec(Delta, Gorilla, LZ4),
    WS50M Float32 Codec(Delta, Gorilla, LZ4),
    WDIR50M Float32 Codec(Delta, Gorilla, LZ4)
) Engine = MergeTree()
primary key (lat, lon)
order by (lat, lon, time);



#bash: for FILENAME in *.parquet; do cat $FILENAME | clickhouse-client --query="insert into test.round_d_lz format Parquet"; done

clickhouse-client --query="select table, name, type, data_compressed_bytes compressed, data_uncompressed_bytes uncompressed, (compressed / uncompressed) ratio, compression_codec codec from system.columns where database like 'test' order by name format TSVWithNames" > /mnt/c/code/wind/merra2_subsetter/clickhouse_compression_test.tsv
