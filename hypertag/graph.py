from pathlib import Path
from igraph import Graph, plot  # type: ignore
from .persistor import Persistor


def graph(layout="fruchterman_reingold", output=None):
    """ Visualize the HyperTag Graph (saved at HyperTagFS root dir) """

    with Persistor() as db:
        if output is None:
            output = str(Path(db.get_hypertagfs_dir()) / "hypertag-graph.pdf")
        db.c.execute(
            """
            SELECT name, tag_id
            FROM tags
            """
        )
        tags = db.c.fetchall()
        tags.sort(key=lambda tup: tup[1])
        tag_ids = sorted([t[1] for t in tags])
        tags = [t[0] for t in tags]
        db.c.execute("SELECT * FROM tags_tags")
        raw_edges = db.c.fetchall()
        print(f"Visualizing HyperTag Graph (V:{len(tags)}, E:{len(raw_edges)})")

    edges: list = []
    edge_id_map: dict = {}
    i = 0
    for tid in tag_ids:
        edge_id_map[tid] = i
        i += 1

    edges = [(edge_id_map[a], edge_id_map[b]) for a, b in raw_edges]
    g = Graph(directed=True)
    g.add_vertices(tags)
    g.add_edges(edges)
    visual_style = {}
    visual_style["bbox"] = (800, 800)
    visual_style["vertex_size"] = 20  # type: ignore
    visual_style["margin"] = 50  # type: ignore
    visual_style["vertex_color"] = "yellow"  # type: ignore
    visual_style["layout"] = g.layout(layout)
    visual_style["vertex_label"] = g.vs["name"]
    plot(g, output, **visual_style)
    print("Visualization saved to", output)


if __name__ == "__main__":
    graph()
