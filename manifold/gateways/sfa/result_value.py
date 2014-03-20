def handle_result_value_geni_3(result_value):
    code = result_value.get('code')
    if not code:
        raise Exception, "Missing code in result value"

    geni_code = code.get('geni_code')
    if geni_code == 0:
        # Success
        return result_value.get_value()
    else:
        # http://groups.geni.net/geni/wiki/GAPI_AM_API_V3/CommonConcepts#ReturnStruct
        output = result_value.get('output')
        raise Exception, output

def handle_result_value(result_value):
    geni_api = result_value.get('geni_api')
    if geni_api != 3:
        raise NotImplemented
    return handle_result_value_geni_3(result_value)

