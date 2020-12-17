# save a few lines of bash
for FILENAME in /mnt/c/data/merra_texas/round/*.parquet; do cat $FILENAME | clickhouse-client --query="insert into test.round_d_lz format Parquet"; done


# SELECT
#     table,
#     name,
#     type,
#     data_compressed_bytes AS compressed,
#     data_uncompressed_bytes AS uncompressed,
#     (compressed / uncompressed) AS ratio,
#     compression_codec AS codec
# FROM system.columns 
# WHERE database like 'test' 
# ORDER BY name
# FORMAT TSVWithNames

clickhouse-client --query="SELECT table, name, type, data_compressed_bytes compressed, data_uncompressed_bytes uncompressed, (compressed / uncompressed) ratio, compression_codec codec from system.columns where database like 'test' order by name format TSVWithNames" > /mnt/c/code/wind/merra2_subsetter/clickhouse_compression_test.tsv