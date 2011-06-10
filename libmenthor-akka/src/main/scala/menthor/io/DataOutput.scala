package menthor.io

import menthor.processing.Vertex

trait DataOutput[Data] extends Serializable {
  def process(vertices: Iterable[Vertex[Data]]): Unit
}
