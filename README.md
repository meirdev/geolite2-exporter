# Export GeoLite2 to CSV files

This script downloads the GeoLite2 databases from [ip-location-db](https://github.com/sapics/ip-location-db) and exports them to CSV files with CIDR notation based on IP ranges.

## Add the databases to ClickHouse

Copy the CSV files to the ClickHouse user files directory (`/var/lib/clickhouse/user_files/`), and create the dictionaries in ClickHouse:

```sql
CREATE DATABASE IF NOT EXISTS dictionaries;

CREATE DICTIONARY IF NOT EXISTS dictionaries.geolite2_asn (
    range String,
    autonomous_system_number UInt32,
    autonomous_system_organization String
)
PRIMARY KEY range
LAYOUT(IP_TRIE())
SOURCE (FILE(path '/var/lib/clickhouse/user_files/geolite2-asn.csv' format 'CSVWithNames'))
LIFETIME(0);

CREATE DICTIONARY IF NOT EXISTS dictionaries.geolite2_country (
    range String,
    country_code String,
    country_name String
)
PRIMARY KEY range
LAYOUT(IP_TRIE())
SOURCE (FILE(path '/var/lib/clickhouse/user_files/geolite2-country.csv' format 'CSVWithNames'))
LIFETIME(0);
```

Using:

```sql
-- Country IPv4
SELECT dictGet('dictionaries.geolite2_country', 'country_code', toIPv4('88.221.111.41'));

-- Country IPv6
SELECT dictGet('dictionaries.geolite2_country', 'country_code', toIPv6('2a02:2f0b:1fe:ffff:ffff:ffff:ffff:ffff'));

-- ASN IPv4
SELECT dictGet('dictionaries.geolite2_asn', 'autonomous_system_number', toIPv4('88.221.111.41'));

-- ASN IPv6
SELECT dictGet('dictionaries.geolite2_asn', 'autonomous_system_number', toIPv6('2a02:2f0b:1fe:ffff:ffff:ffff:ffff:ffff'));
```
