import argparse
import asyncio
import time
import aiohttp
import json
import re
import sys

async def propagate_message(server_id, talks_to_dict, name_port_dict, updated_message):
    local_server_port = name_port_dict[server_id]
    contact_list = talks_to_dict[server_id]
    for contact in contact_list:
        try:
            port = name_port_dict[contact]
            reader, writer = await asyncio.open_connection('127.0.0.1', port)
            print(f"[{server_id}] NEW CONNECTION WITH [{contact}] {port}")
            writer.write(updated_message.encode())
            await writer.drain()
            print(f'[{server_id}] SEND: {updated_message} TO [{contact}] {port}')
            writer.close()
            await writer.wait_closed()
            print(f"[{server_id}] DROP CONNECTION WITH [{contact}] {port}")
        except Exception as e:
            print(f"[{server_id}] ERROR propagate_message(): {e}")


async def handle_IAMAT(reader, writer, server_id, stored_messages, client_message, server_recieved_time,talks_to_dict,name_port_dict,client_port):
    # Parse the message from the client to Server
    tokens = client_message.split()
    command = tokens[0]
    client_id = tokens[1]
    lat_lon = tokens[2]
    client_sent_time = tokens[3]

    # Modified message from server to client.
    command = "AT"
    diff_time = float(server_recieved_time) - float(client_sent_time)
    diff_time_str = diff_time
    if (diff_time >= 0):
        diff_time_str = "+" + str(diff_time)
    else:
        diff_time_str = str(diff_time)
    server_message = " ".join([command, server_id, diff_time_str, client_id, lat_lon, client_sent_time])
    stored_messages[client_id] = server_message #Store server computed message in dictionary
    server_data = server_message.encode()

    writer.write(server_data)  # write message to client
    await writer.drain()  # wait untill all of message is written to client
    print(f"[{server_id}] SEND: {server_message} TO {client_port}")  # print to console message we send to peer

    #Update other servers on change of information
    await propagate_message(server_id,talks_to_dict,name_port_dict,server_message)

    writer.close()  # close connection to client
    await writer.wait_closed()  # wait untill writting strem closed and all data flushed?

async def get_nearby_places_json(location, radius, key, items_bound, server_id):
    try:
        #regex to process location
        lat, lon = re.findall(r"[-+]?\d+\.\d+", location)
        location = f"{float(lat)},{float(lon)}" #Note: float conversion to get rid of "+" URL encoding problem
        #print(location)
        radius = str(int(radius) * 1000)  # km to m conversion

        params = {'location': location, 'radius': radius, 'key': key}
        #print("param: " + params['location'])
        async with aiohttp.ClientSession() as session:
            async with session.get('https://maps.googleapis.com/maps/api/place/nearbysearch/json', params=params) as resp:
                #print(str(resp.url))
                python_object = json.loads(await resp.text())
                python_object['results'] = python_object['results'][:int(items_bound)]
                json_data = json.dumps(python_object, indent=3)
                return json_data
    except Exception as e:
        print(f"[{server_id}] ERROR get_nearby_places_json(): {e}")

async def handle_WHATSAT(reader, writer, server_id, stored_messages, client_message, server_recieved_time,client_port):
    # Parse the message from the client to Server
    tokens = client_message.split()
    command = tokens[0]
    client_id = tokens[1]
    radius = tokens[2]
    items_bound = tokens[3]

    #Search data for string
    server_message = stored_messages.get(client_id,None)
    if(server_message == None):
        #REQUEST IS MADE FOR NONEXISTENT LOCATION? (NOT IN DATA)
        bad_command_message = "? " + client_message
        writer.write(bad_command_message.encode())
        await writer.drain()
        print(f"[{server_id}] SEND: {bad_command_message} TO {client_port}")
    else:
        #Give search keys to Google Places API and write to client after retrieving from stored_messages
        key = 'api-key-here'
        location = (server_message.split())[4]
        nearby_json = await get_nearby_places_json(location, radius, key, items_bound, server_id)
        response_message = server_message + "\n" + nearby_json.rstrip("\n") + "\n\n"
        server_data = response_message.encode()
        writer.write(server_data)
        await writer.drain()
        print(f"[{server_id}] SEND: {response_message} TO {client_port}")

    writer.close()
    await writer.wait_closed()



async def handle_AT(reader, writer, server_id, stored_messages, client_message, server_recieved_time,talks_to_dict,name_port_dict):
    #We only propagate the message to other servers if there is a change in info
    client_id = (client_message.split())[3]
    server_message = stored_messages.get(client_id, None)
    if(server_message == None): #Can't find in stored data so must be new
        stored_messages[client_id] = client_message
        await propagate_message(server_id, talks_to_dict, name_port_dict, client_message)
    elif(server_message != client_message): #Different so have to update and propagate
        stored_messages[client_id] = client_message
        await propagate_message(server_id, talks_to_dict, name_port_dict, client_message)
    #Only case left is when stored_message == client_message so don't have to do anything

async def handle_client(reader, writer, server_id, stored_messages, talks_to_dict,name_port_dict):
    while (reader.at_eof() == False):
        #Handling Recieve client message and then sent back modifed message
        client_data = await reader.readline()
        server_recieved_time = str(time.time())
        client_message = client_data.decode()#decode read bytes into astring
        addr = writer.get_extra_info('peername') #get the ip and port of client
        client_port = addr[1]

        print(f"[{server_id}] NEW CONNECTION WITH {client_port}")
        print(f"[{server_id}] RECIEVED: {client_message} FROM {client_port}") #print to console the read message and ip/port of client

        #Check for command type and handle accordingly
        tokens = client_message.split()
        command = tokens[0]

        match command:
            case "IAMAT":
                await handle_IAMAT(reader, writer, server_id, stored_messages, client_message, server_recieved_time,talks_to_dict,name_port_dict,client_port)
            case "WHATSAT":
                await handle_WHATSAT(reader, writer, server_id, stored_messages, client_message, server_recieved_time,client_port)
            case "AT": #Other servers updating this server with latest info.
                await handle_AT(reader, writer, server_id, stored_messages, client_message, server_recieved_time, talks_to_dict, name_port_dict)
            case _:
                bad_command_message = "? " + client_message
                writer.write(bad_command_message.encode())
                await writer.drain()
                print(f"[{server_id}] SEND: {bad_command_message}")
        writer.close()
        await writer.wait_closed()

    print(f"[{server_id}] DROP CONNECTION WITH {client_port}")

async def main():
    #Logging
    log_file = open("log.txt","a")
    sys.stdout = log_file

    #Arugument Parsing/Handling
    parser = argparse.ArgumentParser(
        prog="server",
        description="Runs server handling client requests",
        epilog="Text at the bottom of help")
    parser.add_argument("server_id")
    args = parser.parse_args()
    #print(args.server_id)

    #Assigned Ports:
    #16465 16466 16467 16468 16469
    #Gradescope Ports:
    #10000 10001 10002 10003 10004
    #Server id's: 'Bailey', 'Bona', 'Campbell', 'Clark', 'Jaquez'

    stored_messages = {} #Messages stored as dictinary of client-id/AT-message
    talks_to_dict = {} #Dictionary of server-name/List of servers server-name talks to
    talks_to_dict['Bailey'] = ['Bona','Campbell']
    talks_to_dict['Bona'] = ['Bailey','Campbell','Clark']
    talks_to_dict['Campbell'] = ['Bailey','Bona','Jaquez']
    talks_to_dict['Clark'] = ['Bona','Jaquez']
    talks_to_dict['Jaquez'] = ['Campbell','Clark']
    name_port_dict = {'Bailey':10000,'Bona':10001,'Campbell':10002,'Clark':10003,'Jaquez':10004} #Hardcode name to port as dict representation

    ip_adress = "127.0.0.1" #localhost
    server_id = args.server_id
    server_port = name_port_dict[server_id]

    server = await asyncio.start_server(
        lambda r,w: handle_client(r, w, server_id, stored_messages,talks_to_dict,name_port_dict), ip_adress, server_port) #start server with the coroutine ip and port

    addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
    print(f'[{server_id}] SERVICING ON {addrs}')

    async with server:
        await server.serve_forever()

    log_file.close()

try:
    asyncio.run(main())
except KeyboardInterrupt:
    print("Server Close")