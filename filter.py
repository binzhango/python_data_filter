def filter(source, output):
    with open(source, 'r') as in_file, open(output, 'w') as out_file:
        messages = []
        global message_head
        message_head = 0
        global line
        for line in in_file:
            flag = check_end_of_message(line)
            if flag == 0 or flag == 1:
                if message_head == 0:
                    for message in messages:
                        out_file.write(message)
                messages.clear()
                message_head = flag
            if message_head == 0:
                messages.append(line)
        #check the messages, if it's not null, flush to file
        if messages:
            for message in messages:
                out_file.write(message)
            messages.clear()
