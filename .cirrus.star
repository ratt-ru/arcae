load("cirrus", "env", "fs", "http")

def main(ctx):
    if env.get("CIRRUS_REPO_FULL_NAME") != "ratt-ru/arcae":
        return []
    
    sha = env.get("CIRRUS_CHANGE_IN_REPO")
    url = "https://api.github.com/repos/ratt-ru/arcae/git/commits/" + sha
    dct = http.get(url).json()

    return fs.read(".cirrus/ci.yml")