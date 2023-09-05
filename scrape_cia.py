import pandas as pd
import requests
import re
from bs4 import BeautifulSoup
from pathlib2 import Path
import datetime


def exports_p_parser(input):
    # recording the content of any notes
    if bool(re.search(r"<strong>.+", input)):
        note = re.search(r"<strong>.+", input)[0]
        note = note.split(">", 2)[-1].rsplit("<", 1)[0].strip()
        input = re.sub(r"<strong>.+", "", input)
    else:
        note = None
    # splitting the cost and year.  Will clean up the year and amount later
    amounts = input.strip("<pbr/>").split("<br/><br/>")
    return amounts, note


def currency_converter(input):
    if not(bool(re.match(r"^\$\d+", input))):
        return None

    di = {"million":  10**6,
          "billion":  10**9,
          "trillion": 10**12}

    input = input.strip("$")
    items = input.split(" ")

    # it is possible for the full amount to be written.  To solve this we will
    # will replace the commas and if the second value is invalid it will multiply
    # by 1.
    # e.g. $2,732,370,000,000 (2020 est.)
    amount = items[0].replace(",", "")
    return str(float(amount) * di.get(items[1], 1))


def import_export_get(url, f_name, skip_links, country_fixes):
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'lxml')

    outputs = []
    for div in soup.findAll("div", {"class": "pb30"}):
        di_out = {}
        try:
            link = div.find("a").get("href")
        except:
            link = ""

        process = (("/the-world-factbook/countries" in link)
                 & (link not in skip_links))
        if process:
            di_out["link"] = link
            di_out["country"] = div.find("a").text
            amounts = div.get_text(strip=True, separator="\n").splitlines()[1:]
            amounts = [a.strip() for a in amounts if a.strip() != ""]
            amounts = [a for a in amounts if bool(re.search(r"\(\d{4}.+\)", a))]
            di_out["amount"] = amounts
            outputs.append(di_out)

    df = pd.DataFrame(outputs)

    mask = df["country"].isin(list(country_fixes.keys()))
    df.loc[mask, "country"] = df.loc[mask, "country"].map(country_fixes)

    df = df.explode("amount").reset_index(drop=True)
    df["amount"].fillna("", inplace=True)
    df["year"] = df["amount"].apply(lambda x: x.split(" (", 1)[-1][:4])
    # some foot notes are being recorded, removing empty years
    mask = df["year"].str.contains("\d{4}", regex=True)
    df.loc[~mask, "year"] = None

    # Originally the currency converter changed data types but for future
    # compatibility that has been taken out.
    mask = df["amount"] != ""
    df.loc[mask, "amount"] = df.loc[mask, "amount"].map(currency_converter)
    df.loc[~mask, "amount"] = None
    df["amount"] = df["amount"].astype(float)

    df["retrieved"] = datetime.datetime.now()
    # just because it is the CIA I will remove the exact time :P
    df["retrieved"] = df["retrieved"].dt.date
    export_file = Path("output", f_name)
    df.to_csv(export_file, index=False)


def partners(url, trade_type, f_name, skip_links, country_fixes):
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'lxml')

    outputs = []
    for div in soup.findAll("div", {"class", "pb30"}):
        di_out = {}
        try:
            link = div.find("a").get("href")
        except:
            link = ""

        process = (("/the-world-factbook/countries" in link)
                 & (link not in skip_links))
        if process:
            di_out["link"] = link
            di_out["country"] = div.find("a").text
            # t for text
            # sometimes a bold or other wrapper appears combining the items in the list
            t = div.get_text(strip=True, separator="\n").splitlines()[1:]
            t = " ".join(t)
            # occasionally get a trailing ,
            t = re.sub(r",\s+\(", " (", t)
            di_out["year"] = t.rsplit("(", 1)[-1][:4]
            di_out["trade_country"] = [c.strip() for c in t.rsplit("(", 1)[0].split(",")]
            outputs.append(di_out)

    df = pd.DataFrame(outputs)
    df = df.explode("trade_country").reset_index(drop=True)
    mask = df["trade_country"].str.contains("%")
    df.loc[mask, "percentage"] = df.loc[mask, "trade_country"].apply(
        lambda x: float(re.search("\d+%$", x)[0][:-1])/100)
    df.loc[mask, "trade_country"] = df.loc[mask, "trade_country"].apply(
        lambda x: x.rsplit(" ", 1)[0]).str.strip()

    # almost entirely is used in some cases, I'll be defining that as 90%
    mask = df["trade_country"].str.contains("almost entirely")
    df.loc[mask, "percentage"] = 0.9
    df.loc[mask, "trade_country"] = df.loc[mask, "trade_country"].apply(
        lambda x: re.sub("almost entirely", "", x).strip())

    mask = df["country"].isin(list(country_fixes.keys()))
    df.loc[mask, "country"] = df.loc[mask, "country"].map(country_fixes)

    mask = df["trade_country"].isin(list(country_fixes.keys()))
    df.loc[mask, "trade_country"] = df.loc[mask, "trade_country"].map(country_fixes)

    df["trade_type"] = trade_type
    df["retrieved"] = datetime.datetime.now()
    # just because it is the CIA I will remove the exact time :P
    df["retrieved"] = df["retrieved"].dt.date

    export_file = Path("output", f_name)
    df.to_csv(export_file, index=False)


def region(url, f_name, skip_links, country_fixes):
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'lxml')

    outputs = []
    for div in soup.findAll("div", {"class", "pb30"}):
        try:
            link = div.find("a").get("href")
        except:
            link = ""

        process = (("/the-world-factbook/countries" in link)
                 & (link not in skip_links))
        if process:
            country = div.get_text(strip=True, separator="\n").splitlines()[0]
            # France has a few region no other countries in multiple regions
            if country=="France":
                list_regions = div.get_text(strip=True, separator="\n").splitlines()[1:]
                list_regions = [r.strip(";").strip() for r in list_regions if ";" in r]
            else:
                list_regions = [div.get_text(strip=True, separator="\n").splitlines()[1]]

            df_foo = pd.DataFrame()
            df_foo["regions"] = list_regions
            df_foo["country"] = country
            df_foo["link"] = link
            df_foo["rank"] = df_foo.index

            outputs.append(df_foo)
            del df_foo

    df = pd.concat(outputs, ignore_index=True, sort=False)

    mask = df["country"].isin(list(country_fixes.keys()))
    df.loc[mask, "country"] = df.loc[mask, "country"].map(country_fixes)

    df["retrieved"] = datetime.datetime.now()
    # just because it is the CIA I will remove the exact time :P
    df["retrieved"] = df["retrieved"].dt.date
    export_file = Path("output", f_name)
    df.to_csv(export_file, index=False)


def trade_goods(url, trade_type, f_name, skip_links, country_fixes):
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'lxml')

    outputs = []

    for div in soup.findAll("div", {"class", "pb30"}):
        try:
            link = div.find("a").get("href")
        except:
            link = ""

        process = (("/the-world-factbook/countries" in link)
                 & (link not in skip_links))
        if process:

            goods = div.get_text(strip=True, separator="\n").splitlines()[1].strip()
            year = goods.rsplit("(", 1)[-1].split(")")[0]
            goods = [g.strip() for g in goods.rsplit("(")[0].split(",")]

            df_foo = pd.DataFrame()
            df_foo["goods"] = goods
            df_foo["country"] = div.find("a").text
            df_foo["link"] = link
            df_foo["year"] = year
            df_foo["rank"] = df_foo.index + 1

            outputs.append(df_foo)

    df = pd.concat(outputs, ignore_index=True, sort=False)

    mask = df["country"].isin(list(country_fixes.keys()))
    df.loc[mask, "country"] = df.loc[mask, "country"].map(country_fixes)

    df["trade_type"] = trade_type

    df["retrieved"] = datetime.datetime.now()
    # just because it is the CIA I will remove the exact time :P
    df["retrieved"] = df["retrieved"].dt.date

    export_file = Path("output", f_name)
    df.to_csv(export_file, index=False)


def population(url, f_name, skip_links, country_fixes):
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'lxml')

    outputs = []

    for div in soup.findAll("div", {"class", "pb30"}):
        try:
            link = div.find("a").get("href")
        except:
            link = ""
        process = (("/the-world-factbook/countries" in link)
                 & (link not in skip_links))

        if process:

            t = div.get_text(strip=True, separator="\n").splitlines()[1:]
            t = " ".join(t)
            matches = re.findall(r"[\d,]+", t)
            di = {"country": div.find("a").text}
            if len(matches) > 0:
                # Most of the countries follow the same format but not all.
                # Akrotiri, Dhekelia
                # This works well with the noteable exceptions of:
                    # French Southern and Antarctic Islands
                    # South Georgia and South Sandwich Islands
                    # United States Pacific Island Wildlife Refuges
                # Given the small size of those it will not be fixed this
                # version
                get_pop  = True
                get_year = True
                while (len(matches) > 0) & get_year:
                    if get_pop:
                        # always be 4 characters while that is impossible for
                        # a population number
                        if len(matches[0]) != 4:
                            di["population"] = matches[0].replace(",","")
                            get_pop = False
                    elif get_year:
                        if len(matches[0]) == 4:
                            di["year"] = matches[0]
                            get_year = False
                    matches = matches[1:]

            outputs.append(di)

    df = pd.DataFrame(outputs)

    mask = df["country"].isin(list(country_fixes.keys()))
    df.loc[mask, "country"] = df.loc[mask, "country"].map(country_fixes)

    df["retrieved"] = datetime.datetime.now()
    # just because it is the CIA I will remove the exact time :P
    df["retrieved"] = df["retrieved"].dt.date

    export_file = Path("output", f_name)
    df.to_csv(export_file, index=False)


def main():
    skip_links = ["/the-world-factbook/countries/",
                  "/the-world-factbook/countries/world",
                  "/the-world-factbook/countries/european-union",
                  "/the-world-factbook/countries/antarctica"
                    ]

    # some of the countries are not standardized or wrong
    di_country_name = {"Korea, South": "South Korea",
                       "Korea, North": "North Korea",
                       "US": "United States",
                       "Untied States": "United States" # my personal favorite
                        }

    url_exports = "https://www.cia.gov/the-world-factbook/field/exports"
    url_exports_partners = "https://www.cia.gov/the-world-factbook/field/exports-partners/"
    url_exports_commodities = "https://www.cia.gov/the-world-factbook/field/exports-commodities/"

    url_imports = "https://www.cia.gov/the-world-factbook/field/imports"
    url_imports_partners = "https://www.cia.gov/the-world-factbook/field/imports-partners/"
    url_imports_commodities = "https://www.cia.gov/the-world-factbook/field/imports-commodities/"

    url_gdp = "https://www.cia.gov/the-world-factbook/field/gdp-official-exchange-rate/"
    url_gdp_real = "https://www.cia.gov/the-world-factbook/field/real-gdp-purchasing-power-parity/"
    url_gdp_real_capita = "https://www.cia.gov/the-world-factbook/field/real-gdp-per-capita/"
    # no longer in use
    # url_gdp_capita = "https://www.cia.gov/the-world-factbook/field/gdp-per-capita/"

    url_population = "https://www.cia.gov/the-world-factbook/field/population"
    url_map_references = "https://www.cia.gov/the-world-factbook/field/map-references"

    # good type still needs to be done
    print("Exports")
    import_export_get(url=url_exports,
                      f_name="exports.csv",
                      skip_links=skip_links,
                      country_fixes=di_country_name)
    print("Exports goods")
    trade_goods(url=url_exports_commodities,
                trade_type="exports",
                f_name="exports_goods.csv",
                skip_links=skip_links,
                country_fixes=di_country_name)
    print("Exports partners")
    partners(url=url_exports_partners,
             trade_type="exports",
             f_name="exports_partners.csv",
             skip_links=skip_links,
             country_fixes=di_country_name)

    print("Imports")
    import_export_get(url=url_imports,
                      f_name="imports.csv",
                      skip_links=skip_links,
                      country_fixes=di_country_name)
    print("Imports goods")
    trade_goods(url=url_imports_commodities,
                trade_type="imports",
                f_name="imports_goods.csv",
                skip_links=skip_links,
                country_fixes=di_country_name)
    print("Imports partners")
    partners(url=url_imports_partners,
             trade_type="imports",
             f_name="imports_partners.csv",
             skip_links=skip_links,
             country_fixes=di_country_name)

    print("GDP")
    import_export_get(url_gdp,
                      f_name="gdp.csv",
                      skip_links=skip_links,
                      country_fixes=di_country_name)
    # print("GDP per capita")
    # import_export_get(url=url_gdp_capita,
                      # f_name="gdp_per_capita.csv",
                      # skip_links=skip_links,
                      # country_fixes=di_country_name)
    print("Real GDP")
    import_export_get(url=url_gdp_real,
                      f_name="real_gdp.csv",
                      skip_links=skip_links,
                      country_fixes=di_country_name)
    print("Real GDP per capita")
    import_export_get(url=url_gdp_real_capita,
                      f_name="real_gdp_per_capita.csv",
                      skip_links=skip_links,
                      country_fixes=di_country_name)


    print("Population")
    population(url=url_population,
               f_name="population.csv",
               skip_links=skip_links,
               country_fixes=di_country_name)
    print("Regions")
    region(url=url_map_references,
           f_name="country_region.csv",
           skip_links=skip_links,
           country_fixes=di_country_name)


if __name__=="__main__":
    print("Downloading data")
    main()
    print("Completed data")
