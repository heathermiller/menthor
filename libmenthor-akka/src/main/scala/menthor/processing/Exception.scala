package menthor.processing

import menthor.MenthorException

class InvalidStepException private[menthor] (message: String) extends MenthorException(message)
class ProcessingException  private[menthor] (message: String, cause: Throwable) extends MenthorException(message, cause)
