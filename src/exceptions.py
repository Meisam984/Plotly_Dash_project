import sys
from src.log import logger


# Exception message customized
def exception_detail(exception: Exception, exc_detail: sys):
    
    tb = exc_detail.exc_info()[2]      #Traceback
    file = tb.tb_frame.f_code.co_filename      # Traceback_file
    module = tb.tb_frame.f_code.co_name     # Traceback module
    line = tb.tb_lineno     # Traceback line
    exception_message = f"Exception raised in {file}, function <<{module}>>, line number {line}: {str(exception)}"

    return exception_message


# Custom Exception class 
class CustomException(Exception):
    def __init__(self, exception: Exception, exc_detail=sys):
        super().__init__(exception)
        self.exception_message = exception_detail(exception, exc_detail)

    def __str__(self):
        logger.exception(msg=self.exception_message, exc_info=False)
        return self.exception_message
    