import nx

def run():
    nx.log("Hello from Python guest!")
    nx.db_set("hello", "numax-python")
    nx.log("db_set ok")


run()