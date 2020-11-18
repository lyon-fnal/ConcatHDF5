using Documenter, IRMA

makedocs(
    sitename = "IRMA Docs",
    format = Documenter.HTML(),
    modules = IRMA,
    authors = "Adam L. Lyon",
    clean = true,
    pages = [
        "index.md",
        "tips.md",
        "shist.md",
        "partitionDS.md",
        "stopwatch.md",
        "mpi.md",
        "api.md",
    ]
)

deploydocs(
    repo = "github.com/lyon-fnal/IRMA.jl.git",
)

# Documenter can also automatically deploy documentation to gh-pages.
# See "Hosting Documentation" and deploydocs() in the Documenter manual
# for more information.
#=deploydocs(
    repo = "<repository url>"
)=#
