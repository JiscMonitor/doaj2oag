import esprit

local = esprit.raw.Connection("http://localhost:9200", "doaj")
ooz = esprit.raw.Connection("http://ooz.cottagelabs.com:9200", "monitor")

esprit.tasks.copy(local, "article", ooz, "article")