def main(args):
    name = args.get("name", "stranger")
    greeting = "Hello, " + name + "!"
    return {"greeting": greeting}