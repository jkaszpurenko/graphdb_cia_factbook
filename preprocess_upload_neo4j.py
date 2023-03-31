"""
Script takes data from the scrape_cia.  Cleans and combines it for nodes and
edges to be uploaded into Neo4j.  Then:

Neo4j will calculate the pageRank and add to the nodes
Neo4j will calculate the articleRank and add to the nodes

This information and the files used to upload the nodes and edges will be
exported to:
    output/article_page_rank_countries.csv
    output/trade_partners.csv
"""
import pandas as pd
from pathlib2 import Path
from getpass import getpass
import py2neo
from json import dumps


# ==============================================================================
# This is currently configured for a local host of neo4j.
#
#
#                       Remember to start neo4j
# ==============================================================================


def main():
    # Output files
    f_out_country  = Path("output", "article_page_rank_countries.csv")
    f_out_trade    = Path("output", "trade_partners.csv")

    # Erase all existing data
    erase_existing_neo4j = True

    # default Neo4j host
    url = "bolt://localhost:7687"
    username = input("Username: ")
    print("\n")
    password = getpass()

    graph = py2neo.Graph(url, auth=(username, password))

    # Check the existing constraints
    current_constraints = str(graph.run("""CALL db.constraints"""))

    if ":country {name" not in current_constraints:
        graph.run("""CREATE CONSTRAINT ON (n:country) ASSERT n.name IS NODE KEY""")

    if ":region {name" not in current_constraints:
        graph.run("""CREATE CONSTRAINT ON (n:region) ASSERT n.name IS NODE KEY""")

    if ":good {name" not in current_constraints:
        graph.run("""CREATE CONSTRAINT ON (n:good) ASSERT n.name IS NODE KEY""")

    # deletes the existing database
    if erase_existing_neo4j:
        graph.run("""MATCH (n) DETACH DELETE n""")

    print("DB connected to and conditions verified")
    # List of files.  With the exception of goods_grouping the rest are obtained
    # by running scrape_cia.py
    # goods_grouping.csv was created by myself
    f_exports = Path("output", "exports.csv")
    f_exports_goods = Path("output", "exports_goods.csv")
    f_exports_partners = Path("output", "exports_partners.csv")

    f_imports = Path("output", "imports.csv")
    f_imports_goods = Path("output", "imports_goods.csv")
    f_imports_partners = Path("output", "imports_partners.csv")

    f_gdp = Path("output", "gdp.csv")
    f_gdp_capita = Path("output", "gdp_per_capita.csv")
    f_real_gdp = Path("output", "real_gdp.csv")
    f_real_gdp_capita = Path("output", "real_gdp_per_capita.csv")

    f_goods_group = Path("output", "goods_grouping.csv")
    f_pop = Path("output", "population.csv")
    f_region = Path("output", "country_region.csv")

    # Automatic data types works but I've had some bad experiences so I like to
    # manually specify.  Retrieved is not used as a date so not parsing
    di_types = {"link": str,
                "country": str,
                "amount": float,
                "note": str,
                "year": float,
                "trade_country": str,
                "percentage": float,
                "trade_type": str,
                "goods": str,
                "mapped_good": str,
                "rank": int,
                "population": float,
                "regions": str,
                "retrieved": str
                }
    df_exp = pd.read_csv(f_exports, dtype=di_types)
    df_exp_part = pd.read_csv(f_exports_partners, dtype=di_types)
    df_exp_good = pd.read_csv(f_exports_goods, dtype=di_types)

    df_imp = pd.read_csv(f_imports, dtype=di_types)
    df_imp_good = pd.read_csv(f_imports_goods, dtype=di_types)
    df_imp_part = pd.read_csv(f_imports_partners, dtype=di_types)

    df_gdp = pd.read_csv(f_gdp, dtype=di_types)
    df_gdp_capita = pd.read_csv(f_gdp_capita, dtype=di_types)
    df_real_gdp = pd.read_csv(f_real_gdp, dtype=di_types)
    df_real_gdp_capita = pd.read_csv(f_real_gdp_capita, dtype=di_types)

    df_pop = pd.read_csv(f_pop, dtype=str)
    df_goods_group = pd.read_csv(f_goods_group, dtype=str)
    df_region = pd.read_csv(f_region, dtype=di_types)

    print("Files read")

    # Preproccessing
    # Creating the country table
    df_country = df_region.loc[df_region["rank"] == 0].copy()
    df_country = df_country.reset_index(drop=True)

    cols = ["country", "population", "year"]
    df_country = pd.merge(df_country, df_pop[cols], on="country", how="left")

    df_country.rename(columns={"year": "year_population"}, inplace=True)
    df_country["year_population"].fillna(1970, inplace=True)
    df_country["population"].fillna(0, inplace=True)

    pairs = [(df_exp, "exports"),
             (df_imp, "imports"),
             (df_gdp, "gdp"),
             (df_gdp_capita, "gdp_per_capital"),
             (df_real_gdp, "real_gdp"),
             (df_real_gdp_capita, "real_gdp_per_capita")
           ]
    for df_frame, name in pairs:
        di = {"amount": "amount_" + name,
              "year": "year_" + name}
        cols = ["country"] + list(di.values())
        df_foo = df_frame.copy()
        df_foo.sort_values("year", ascending=False, inplace=True)
        df_foo.drop_duplicates("country", inplace=True)
        df_foo.rename(columns=di, inplace=True)
        df_country = pd.merge(df_country, df_foo[cols], on="country", how="left")
        df_country[di["amount"]].fillna(0, inplace=True)
        df_country[di["year"]].fillna(1970, inplace=True)


    # Creating a trade data set, this will act as the edges and combine both
    # imports and exports
    # estimates for the most recent year
    cols = ["regions", "country"]
    df_region = df_region.drop_duplicates(cols, keep="first").reset_index(drop=True)

    df_exp_good["year"].fillna(1970, inplace=True)
    df_imp_good["year"].fillna(1970, inplace=True)

    df_exp_good = pd.merge(df_exp_good, df_goods_group, how="left", on="goods")
    df_imp_good = pd.merge(df_imp_good, df_goods_group, how="left", on="goods")

    df_exp.sort_values("year", ascending=False, inplace=True)
    df_foo_exp = df_exp.drop_duplicates("country", keep="first").copy()

    df_exp_part = pd.merge(df_exp_part, df_foo_exp[["country", "amount"]], how="left", on="country")
    df_exp_part["amount"] = df_exp_part["amount"] * df_exp_part["percentage"]
    di_foo = {"country": "exports", "trade_country": "imports"}
    df_exp_part.rename(columns=di_foo, inplace=True)

    df_imp.sort_values("year", ascending=False, inplace=True)
    df_foo_imp = df_imp.drop_duplicates("country", keep="first").copy()

    df_imp_part = pd.merge(df_imp_part, df_foo_imp[["country", "amount"]], how="left", on="country")
    df_imp_part["amount"] = df_imp_part["amount"] * df_imp_part["percentage"]

    di_foo = {"country": "imports", "trade_country": "exports"}
    df_imp_part.rename(columns=di_foo, inplace=True)

    df_trade = pd.concat([df_exp_part, df_imp_part], ignore_index=True, sort=False)
    # Some extra comma's in the formatting CIA's web page causing issues
    mask = df_trade["imports"].notnull() & df_trade["exports"].notnull()
    df_trade = df_trade.loc[mask].reset_index(drop=True)

    df_trade.sort_values(["year", "amount", "trade_type"], ascending=[False, False, True], inplace=True)
    df_trade.drop_duplicates(["imports", "exports"], inplace=True)
    df_trade["amount"].fillna(0, inplace=True)
    df_trade["export_trade_rank"] = df_trade.groupby("exports")["amount"].rank("min", ascending=False)
    df_trade["import_trade_rank"] = df_trade.groupby("imports")["amount"].rank("min", ascending=False)

    df_trade["year"].fillna(1970, inplace=True)
    df_trade.reset_index(drop=True, inplace=True)

    # percentages is confusing because is it the percentage for exporting or
    # importing country.  Will redo that with the percentage of exporting and
    # importing
    df_trade.drop("percentage", axis=1, inplace=True)
    di_exp = dict(zip(df_country["country"], df_country["amount_exports"]))
    di_imp = dict(zip(df_country["country"], df_country["amount_imports"]))

    df_trade["percentage_exports"] = df_trade["amount"] / df_trade["exports"].map(di_exp)
    df_trade["percentage_exports"].fillna(0, inplace=True)

    df_trade["percentage_imports"] = df_trade["amount"] / df_trade["imports"].map(di_imp)
    df_trade["percentage_imports"].fillna(0, inplace=True)


    # Goods
    cols = ["goods", "mapped_good"]
    df_good = pd.concat([df_exp_good[cols], df_imp_good[cols]], ignore_index=True)
    df_good = df_good.groupby("mapped_good")["goods"].unique().apply(list)
    df_good = df_good.reset_index()

    print("Files preprocessed and uploading to Neo4j")
    # ==========================================================================
    # Upload data sets to Neo4j
    # If you upload the csv into the folder associated with the neo4j project
    # it is faster but given the relative small scale of this project I've
    # elected to upload manually.

    print("Uploading COUNTRY nodes to Neo4j")
    # upload node countries
    for row in df_country.index:
        cols = ["link",
                "country",
                "amount_exports",
                "year_exports",
                "amount_imports",
                "year_imports",
                "regions",
                "retrieved",
                "amount_gdp",
                "year_gdp",
                "amount_gdp_per_capital",
                "amount_real_gdp",
                "year_real_gdp",
                "amount_real_gdp_per_capita",
                "population",
                "year_population"]
        foo = df_country.loc[row, cols]
        di = dict(zip(foo.index, foo.values))
        cql = """
                MERGE (n:country
                        {{name: "{country}",
                          link: "{link}",
                          amount_export: {amount_export},
                          year_export: {yr_export},
                          amount_import: {amount_import},
                          year_import: {yr_import},
                          primary_region: "{region}",
                          gdp: {gdp},
                          gdp_per_capita: {gdp_per_capita},
                          year_gdp: {yr_gdp},
                          real_gdp: {real_gdp},
                          real_gdp_per_capita: {real_gdp_capita},
                          year_real_gdp: {yr_real_gdp},
                          population: {pop},
                          year_population: {yr_pop},
                          date_retrieved: TIMESTAMP("{retrieved}")
                }}
                )
        """.format(
                   country=di["country"],
                   link=di["link"].strip("/"),
                   amount_export=round(di["amount_exports"] / 1000000000, 3),
                   yr_export=di["year_exports"],
                   amount_import=round(di["amount_imports"] / 1000000000, 3),
                   yr_import=di["year_imports"],
                   region=di["regions"],
                   gdp=round(di["amount_gdp"] / 1000000000, 3),
                   gdp_per_capita=di["amount_gdp_per_capital"],
                   yr_gdp=di["year_gdp"],
                   real_gdp=round(di["amount_real_gdp"] / 1000000000, 3),
                   real_gdp_capita=di["amount_real_gdp_per_capita"],
                   yr_real_gdp=di["year_real_gdp"],
                   pop=di["population"],
                   yr_pop=di["year_population"],
                   retrieved=di["retrieved"]
                   )
        graph.run(cql)

    print("Uploading TRADES edges to Neo4j")
    # Upload edges trade amounts
    for row in df_trade.index:
        cols = ["exports",
                "imports",
                "percentage_exports",
                "percentage_imports",
                "year",
                "amount",
                "trade_type",
                "export_trade_rank",
                "import_trade_rank",
                "retrieved"]
        foo = df_trade.loc[row, cols]
        di = dict(zip(foo.index, foo.values))

        cql = """
                MATCH (n:country {{name: "{src_country}"}}), (m:country {{name: "{dest_country}"}})
                MERGE (n)-[e:trades {{amount: {amount}, year:{year}, percentage_exports: {percent_exp}, percentage_imports: {percent_imp}, export_trade_rank: {export_rank}, import_trade_rank: {import_rank}, trade_source: "{trade_type}", retrieved: TIMESTAMP("{retrieved}")}}]->(m)
        """.format(src_country=di["exports"],
                   dest_country=di["imports"],
                   amount=round(di["amount"] / 1000000000, 3),
                   year=di["year"],
                   percent_exp=di["percentage_exports"],
                   percent_imp=di["percentage_imports"],
                   trade_type=di["trade_type"],
                   export_rank=di["export_trade_rank"],
                   import_rank=di["import_trade_rank"],
                   retrieved=di["retrieved"]
                   )
        graph.run(cql)

    print("Uploading REGION nodes to Neo4j")
    # upload node regions
    for r in df_region["regions"].unique():
        cql = """
                MERGE (n:region
                        {{name: "{region}"
                }}
                )
        """.format(region=r)
        graph.run(cql)

    print("Uploading CONTAINS edges to Neo4j")
    # upload edge located in
    for row in df_region.index:
        cols = ["regions",
                "country",
                "rank",
                "retrieved"]
        foo = df_region.loc[row, cols]
        di = dict(zip(foo.index, foo.values))

        cql = """
                MATCH (n:region {{name: "{region}"}}), (m:country {{name: "{country}"}})
                MERGE (n)-[e:contains {{rank: {rank}, retrieved: TIMESTAMP("{retrieved}")}}]->(m)
        """.format(region=di["regions"],
                   country=["country"],
                   rank=di["rank"],
                   retrieved=di["retrieved"]
                   )
        graph.run(cql)

    print("Uploading GOOD nodes to Neo4j")
    # uploading goods
    for row in df_good.index:
        mapped_good, goods = df_good.loc[row, ["mapped_good", "goods"]]
        cql = """
                MERGE (n:good {{name: "{mapped_good}", sub_goods: {goods}}})""".format(
                    mapped_good=mapped_good,
                    goods=goods)
        graph.run(cql)

    print("Uploading EXPORTS edges to Neo4j")
    # uploading goods to exports
    for row in df_exp_good.index:
        cols = ["goods",
                "mapped_good",
                "country",
                "rank",
                "year",
                "retrieved"]
        foo = df_exp_good.loc[row, cols]
        di = dict(zip(foo.index, foo.values))
        cql = """
                MATCH (g:good {{name: "{mapped_good}"}}), (c:country {{name: "{country}"}})
                MERGE (c)-[e:exports {{rank: {rank}, year: {year}, sub_good: "{good}", retrieved: TIMESTAMP("{retrieved}")}}]->(g)
        """.format(mapped_good=di["mapped_good"],
                   good=di["goods"],
                   country=di["country"],
                   rank=di["rank"],
                   year=di["year"],
                   retrieved=di["retrieved"]
                   )
        graph.run(cql)

    print("Uploading IMPORTS edges to Neo4j")
    # uploading goods to imports
    for row in df_imp_good.index:
        cols = ["goods",
                "mapped_good",
                "country",
                "rank",
                "year",
                "retrieved"]
        foo = df_imp_good.loc[row, cols]
        di = dict(zip(foo.index, foo.values))

        # MERGE is giving a few errors, need to solve that.  CREATE will throw an
        # error if the node exists
        cql = """
                MATCH (g:good {{name: "{mapped_good}"}}), (c:country {{name: "{country}"}})
                MERGE (g)-[e:imports {{rank: {rank}, year: {year}, sub_good: "{good}", retrieved: TIMESTAMP("{retrieved}")}}]->(c)
        """.format(mapped_good=di["mapped_good"],
                   good=di["goods"],
                   country=di["country"],
                   rank=di["rank"],
                   year=di["year"],
                   retrieved=di["retrieved"]
                   )
        graph.run(cql)

    print("Nodes and Edges uploaded")
    # ==============================================================================
    # Page Rank
    print("Calculating pageRank")
    cql_config = """CALL gds.graph.project(
      'myGraph',
      'country',
      'trades',
      {
        relationshipProperties: 'amount'
      }
    )"""
    graph.run(cql_config)

    # Not needed since not that big but good practice to check
    cql = """CALL gds.pageRank.write.estimate('myGraph', {
      writeProperty: 'pageRank',
      maxIterations: 20,
      dampingFactor: 0.85
    })
    YIELD nodeCount, relationshipCount, bytesMin, bytesMax, requiredMemory"""
    foo = graph.run(cql)

    # add PageRank
    cql = """
    CALL gds.pageRank.mutate('myGraph', {
      maxIterations: 20,
      dampingFactor: 0.85,
      mutateProperty: 'pagerank'
    })
    YIELD nodePropertiesWritten, ranIterations
    """
    page_scores = graph.run(cql)

    # writing PageRank
    cql = """CALL gds.pageRank.write('myGraph', {
      maxIterations: 20,
      dampingFactor: 0.85,
      writeProperty: 'pagerank'
    })
    YIELD nodePropertiesWritten, ranIterations"""
    graph.run(cql)

    print("Calculating articleRank")
    # Writing ArticleRank
    cql = """CALL gds.articleRank.write('myGraph', {
      writeProperty: 'articlerank'
    })
    YIELD nodePropertiesWritten, ranIterations
    """
    graph.run(cql)

    # Get the pageranks
    cql = """MATCH (n:country)
           RETURN n.name AS country, n.pagerank AS page_rank, n.articlerank AS article_rank"""
    page_ranks = dumps(graph.run(cql).data())
    df_foo = pd.DataFrame(eval(page_ranks))

    df_country = pd.merge(df_country, df_foo, how="left")

    print("Exporting Files")
    # Exports
    df_country.sort_values("page_rank", ascending=False, inplace=True)
    df_country.to_csv(f_out_country, index=False)

    df_trade.to_csv(f_out_trade, index=False)


if __name__=="__main__":
    main()
