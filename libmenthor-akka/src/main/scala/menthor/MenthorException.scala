package menthor

import akka.AkkaException

class MenthorException(message: String = "", cause: Throwable = null) extends AkkaException(message, cause)
