import ipaddress

import polars as pl
import pycountry


def convert_to_cidr(x):
    return list(
        map(
            str,
            ipaddress.summarize_address_range(
                ipaddress.ip_address(x["ip_range_start"]),
                ipaddress.ip_address(x["ip_range_end"]),
            ),
        )
    )


def get_country_name(x):
    try:
        return pycountry.countries.get(alpha_2=x).name
    except AttributeError:
        return ""


country_columns = ["ip_range_start", "ip_range_end", "country_code"]

print("Downloading geolite2-country-ipv4.csv")

df_country_ipv4 = pl.read_csv(
    "https://cdn.jsdelivr.net/npm/@ip-location-db/geolite2-country/geolite2-country-ipv4.csv",
    new_columns=country_columns,
    has_header=False,
)

print("Downloading geolite2-country-ipv6.csv")

df_country_ipv6 = pl.read_csv(
    "https://cdn.jsdelivr.net/npm/@ip-location-db/geolite2-country/geolite2-country-ipv6.csv",
    new_columns=country_columns,
    has_header=False,
)

print("Converting ip_range_start and ip_range_end to CIDR")

df = pl.concat([df_country_ipv4, df_country_ipv6])

df = df.with_columns(
    pl.struct("ip_range_start", "ip_range_end")
    .map_elements(convert_to_cidr, return_dtype=pl.List(pl.String))
    .alias("range")
)

df = df.with_columns(
    df["country_code"].map_elements(get_country_name, return_dtype=pl.String).alias("country_name")
)

df = df.drop(["ip_range_start", "ip_range_end"])

df = df.explode("range")

df.write_csv("./geolite2-country.csv")


asn_columns = [
    "ip_range_start",
    "ip_range_end",
    "autonomous_system_number",
    "autonomous_system_organization",
]

print("Downloading geolite2-asn-ipv4.csv")

df_country_ipv4 = pl.read_csv(
    "https://cdn.jsdelivr.net/npm/@ip-location-db/geolite2-asn/geolite2-asn-ipv4.csv",
    new_columns=asn_columns,
    has_header=False,
)

print("Downloading geolite2-asn-ipv6.csv")

df_country_ipv6 = pl.read_csv(
    "https://cdn.jsdelivr.net/npm/@ip-location-db/geolite2-asn/geolite2-asn-ipv6.csv",
    new_columns=asn_columns,
    has_header=False,
)

print("Converting ip_range_start and ip_range_end to CIDR")

df = pl.concat([df_country_ipv4, df_country_ipv6])

df = df.with_columns(
    pl.struct("ip_range_start", "ip_range_end")
    .map_elements(convert_to_cidr, return_dtype=pl.List(pl.String))
    .alias("range")
)

df = df.drop(["ip_range_start", "ip_range_end"])

df = df.explode("range")

df.write_csv("./geolite2-asn.csv")
