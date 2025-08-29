import asyncio
import json
from json import JSONDecodeError

import websockets
import aiohttp
import uuid
from datetime import datetime
import polars as pl


class BotFrameworkClient:
    def __init__(self, bot_token, crosscutting_token):
        self.bot_token = bot_token
        self.crosscutting_token = crosscutting_token
        self.conversation_id = None
        self.stream_url = None
        self.websocket = None
        self.answers = []

    async def get_direct_line_token(self):
        """Get DirectLine token using bot token"""
        url = "https://directline.botframework.com/v3/directline/tokens/generate"
        headers = {
            "Authorization": f"Bearer {self.bot_token}",
            "Content-Type": "application/json"
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("token")
                else:
                    raise Exception(f"Failed to get DirectLine token: {response.status}")

    async def start_conversation(self, directline_token):
        """Start a new conversation"""
        url = "https://directline.botframework.com/v3/directline/conversations"
        headers = {
            "Authorization": f"Bearer {directline_token}",
            "Content-Type": "application/json"
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers) as response:
                if response.status == 201:
                    data = await response.json()
                    self.conversation_id = data.get("conversationId")
                    self.stream_url = data.get("streamUrl")
                    return data
                else:
                    raise Exception(f"Failed to start conversation: {response.status}")

    async def connect_websocket(self):
        """Connect to the WebSocket stream"""
        if not self.stream_url:
            raise Exception("No stream URL available. Start conversation first.")

        self.websocket = await websockets.connect(self.stream_url)
        print(f"Connected to WebSocket: {self.stream_url}")

    async def send_message(self, text, directline_token):
        """Send a message to the bot"""
        url = f"https://directline.botframework.com/v3/directline/conversations/{self.conversation_id}/activities"
        headers = {
            "Authorization": f"Bearer {directline_token}",
            "Content-Type": "application/json"
        }

        activity = {
            "type": "message",
            "from": {
                "id": "15864749",
                "name": "15864749",
                "role": "user"
            },
            "channelId": "directline",
            "channelData": {
                "x-unir-crosscutting-token": self.crosscutting_token,
                "x-unir-domain": "stubot2tst.blob.core.windows.net",
                "x-unir-impersonated-student-id": "783198",
                "x-unir-impersonated-platform": "sin asignar",
                "x-unir-impersonated-domain": "moodle.preunir.net",
                "x-unir-platform": "sin asignar",
                "x-unir-current-user-id": "",
                "x-unir-student-id": "15864749",
                "x-unir-student-lastname": "Pérez Sechi",
                "x-unir-student-name": "Carlos Ignacio",
                "x-unir-user-timezone-offset": -120
            },
            "locale": "es-ES",
            "text": text,
            "conversation": {
                "id": self.conversation_id
            },
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=activity) as response:
                if response.status == 200:
                    data = await response.json()
                    print(f"Message sent: {text}")
                    return data
                else:
                    error_text = await response.text()
                    print(f"HTTP Error {response.status}: {error_text}")
                    print(f"Request URL: {url}")
                    print(f"Request headers: {headers}")
                    raise Exception(f"Failed to send message: {response.status} - {error_text}")

    async def listen_for_messages(self):
        """Listen for incoming messages from the WebSocket"""
        try:
            async for message in self.websocket:
                try:
                    if isinstance(message, (bytes, bytearray)):
                        message = message.decode("utf-8")  # ensure UTF-8 decoding

                    data = json.loads(message)
                except json.JSONDecodeError as json_error:
                    print(f"Failed to decode JSON: {json_error}")
                    self.answers.append("decode error")
                    continue
                except UnicodeDecodeError as e:
                    print(f"Encoding error: {e}")
                    continue


                if "activities" in data:
                    for activity in data["activities"]:
                        # Only process bot messages (not our own user messages)
                        if (activity.get("type") == "message" and
                                activity.get("from", {}).get("id") != "15864749"):

                            # Display main message text
                            bot_text = activity.get("text", "")
                            if bot_text:
                                print(bot_text)
                                self.answers.append(bot_text)

        except websockets.exceptions.ConnectionClosed:
            print("WebSocket connection closed")

        except Exception as e:
            print(f"Error listening for messages: {e}")

    async def close(self):
        """Close the WebSocket connection"""
        if self.websocket:
            await self.websocket.close()


async def main():
    # Bot token
    bot_token = "dGFpQSOk5VA.UUEkyKPpQ8e8w6X6xG6WAj660AIsr8zc7yjHO56LNws"
    crosscutting_token = "YxXUHWqjHnHdD3zj3PRiPUYc3uVNnkOXOx5R59yt5CJ0s_bhSTOaf0DGQ4FFchQC6N3mGI715ZfFC1gaTPeXP2qmNkB8Vm1dOe2S-QfnO5Z0pjuSgs0A5LRgRf9xo4P9OzkOx3QMkqSLBa7ClZD80uT0GuSoh2EXFslUKPFhNk2QjDT3DRUnwIpCXfOOUZeZXlIvZ3dWNwU6rO8HTaNTWyJip5U1okCTTtLsv6JD4QAluYogr9NeseyA3vDN8RVQ8eb7vfAuQxiOoyxPB4looLNKWQGvNHpp5GCqbEPaFtyMDifGCrxXxHZvM4rzk59HgusD6BD6nuNujq2tgUZxH2HUqAoAn88S9iVZ-WvX9Q2FRkBanuzHhZmOJlTvjlPRtut0zpdnnpF4KTqiCnIJtx5KiGya99ZNlz8OfJelpTAmBk4HTQ5izYT1eCFLNPaAvKMlXFB9qK4ymtbMcsDB02ZgeTimgtNHoxYU5UToe2J59b2Sd4ZvNmw_qtjgCjqoytPhLhKFgugmqeScr46caOO3_awxkwtxy7Q132yABAI5Ry8g9shCWiGkf_ol2kVk"

    client = BotFrameworkClient(bot_token, crosscutting_token)

    try:

        # Get DirectLine token
        print("Getting DirectLine token...")
        directline_token = await client.get_direct_line_token()
        print("DirectLine token obtained")

        # Start conversation
        print("Starting conversation...")
        conversation_data = await client.start_conversation(directline_token)
        print(f"Conversation started: {client.conversation_id}")
        print(f"Stream URL: {client.stream_url}")

        # Validate conversation setup
        if not client.conversation_id:
            raise Exception("Failed to get conversation ID")
        if not client.stream_url:
            raise Exception("Failed to get stream URL")

        # Connect to WebSocket
        print("Connecting to WebSocket...")
        await client.connect_websocket()

        # Start listening for messages in background
        listen_task = asyncio.create_task(client.listen_for_messages())

        # Send initial message
        # await client.send_message("Hello, bot!", directline_token)

        # Interactive loop for sending messages
        df = pl.read_excel("Tests 1.xlsx")
        df_list = df["QUESTION"].to_list()
        print(len(df_list))
        for q in df_list:
            await client.send_message(q, directline_token)

            # Give some time for the bot to respond
            await asyncio.sleep(5)
        messages_list = client.answers

        # Cancel listening task and close connection
        listen_task.cancel()
        await client.close()

        with open("answers.txt", "w") as answers:
            answers.writelines(ml for ml in messages_list)
            answers.close()

        df_answers = pl.DataFrame({
            "QUESTION": messages_list,
            "ANSWER": df["QUESTION"].to_list()
        })
        df_answers.write_excel("answers.xlsx")
    except Exception as e:
        print(f"Error: {e}")
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())