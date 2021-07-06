from datetime import datetime
import logging


def get_log(log_level,request,error):
    curr_time = datetime.now().strftime(format="%y-%m-%d %H:%M:%S")
    if log_level==logging.INFO :
        return f"Address: {request.remote_addr} - Request Path {request.path} - Time {curr_time}"                        
    elif log_level == logging.ERROR or log_level==logging.WARN:
        return f"Address: {request.remote_addr} - Request Path: {request.path} - Time: {curr_time} - Reason: {error}"