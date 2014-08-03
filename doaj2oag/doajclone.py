import esprit
from portality.core import app

doaj = esprit.raw.Connection("http://doaj.org", "query", port=80)
local = esprit.raw.Connection("http://localhost:9200", "doaj")

query = {
    "query" : {
        "term" : {
            "bibjson.identifier.type.exact" : "doi"
        }
    }
}

esprit.tasks.copy(doaj, "journal,article", local, "article", limit=100000, method="GET", q=query)