package example1

object Main {

  def main (args: Array [String]) {
    if (args.length > 0 && args (0) == "init")
      new Initializer () .main (args.slice (1, args.length))
    else
      new Server () .main (args)
  }}
