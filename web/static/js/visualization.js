function make_ajax_call(type, url, onSuccess){
	/* Would only work for GET, need to modify for POST send_data */
	
	$.ajax({
		type: type,
		url: url,
		data: send_data,
		dataType: "json",
		contentType: 'application/json;charset=UTF-8',
		success: function(return_data){
			onSuccess(return_data)
		}
	})
}