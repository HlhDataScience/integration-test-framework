import json
import time
import asyncio
import aiohttp  # type: ignore
import websockets  # type: ignore
import polars as pl  # type: ignore
from datetime import datetime, UTC
from websockets.asyncio.client import connect  # type: ignore

async def get_direct_line_token(bot_token) -> str:
    """Get DirectLine token using bot token"""
    url = "https://directline.botframework.com/v3/directline/tokens/generate"
    headers = {
        "Authorization": f"Bearer {bot_token}",
        "Content-Type": "application/json"
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                return data.get("token")
            else:
                raise Exception(
                    f"Failed to get DirectLine token: {response.status}")


async def start_conversation(directline_token) -> dict:
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
                return data
            else:
                raise Exception(
                    f"Failed to start conversation: {response.status}")


async def send_message(conversation_id, text, directline_token, crosscutting_token):
    """Send a message to the bot"""
    url = f"https://directline.botframework.com/v3/directline/conversations/{conversation_id}/activities"
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
            "x-unir-crosscutting-token": crosscutting_token,
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
            "id": conversation_id
        },
        "timestamp": datetime.now(UTC).isoformat() + "Z"
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=activity) as response:
            if response.status == 200:
                return await response.json()
            else:
                error_text = await response.text()
                print(f"HTTP Error {response.status}: {error_text}")
                raise Exception(
                    f"Failed to send message: {response.status} - {error_text}")


async def listen_for_messages(websocket, question, timeout) -> list[dict]:
    """Listen for incoming messages from the WebSocket with improved pairing"""
    answers = []
    start_time = time.time()
    over = False
    try:
        while not over:
            elapsed_time = time.time() - start_time
            over = elapsed_time >= timeout
            if over:
                break

            remaining_time = timeout - elapsed_time
            try:

                message = await asyncio.wait_for(
                    websocket.recv(),
                    timeout=min(1.0, remaining_time)
                )

                if isinstance(message, (bytes, bytearray)):
                    message = message.decode("utf-8")

                data = json.loads(message)
            except asyncio.TimeoutError:
                continue
            except json.JSONDecodeError as json_error:
                print(f"Failed to decode JSON for question {question}: {json_error}")
                continue
            except UnicodeDecodeError as e:
                print(f"Encoding error: {e}")
                continue

            if "activities" in data:
                for activity in data["activities"]:
                    if activity.get("type") == "message":
                        message_text = activity.get("text", "")
                        sender_role = activity.get("from", {}).get("role")
                        sender_role = sender_role if sender_role else "unknown"
                        if sender_role != "user":
                            answers.append({
                                "question": question,
                                "answer": message_text
                            })
                            print(f"")
                            print(f"{question}: {message_text}")
                            print(f"")
                            over = True

    except websockets.exceptions.ConnectionClosed:
        print("WebSocket connection closed")
    except Exception as e:
        print(f"Error listening for messages: {e}")

    return answers


async def process_question(question, bot_token, crosscutting_token, answers, answers_lock):
    try:
        directline_token = await get_direct_line_token(bot_token)
        conversation = await start_conversation(directline_token)


        async with connect(conversation['streamUrl']) as websocket:
            await send_message(
                conversation['conversationId'],
                question,
                directline_token,
                crosscutting_token
            )
            question_answers = await listen_for_messages(
                websocket,
                question,
                timeout=120
            )
            async with answers_lock:
                answers.extend(question_answers)
    except Exception as e:
        print(f"Error processing question '{question}': {e}")


async def worker(queue, bot_token, crosscutting_token, answers, answers_lock):
    while True:
        question = await queue.get()
        if question is None:  # Sentinel to stop worker
            queue.task_done()
            break
        print(f"Worker started for question: {question}")
        try:

            await process_question(question, bot_token, crosscutting_token, answers, answers_lock)
            queue.task_done()
        except Exception as e:
            print(f"Worker error: {e}")
            queue.task_done()


async def main():
    # Bot token and configuration
    bot_token = "dGFpQSOk5VA.UUEkyKPpQ8e8w6X6xG6WAj660AIsr8zc7yjHO56LNws"
    crosscutting_token = "13DQpoa0MWw4Z-WQpJgNB6FEB6x4YV1WX3WFZE1YgAf6ErxZgCmevCzzGwrer0oxAutwgyOsBNBNJ10eEcZroZqXLbKpxw70ukRtns1BtQxPc4mNmJS5aczEzRr1k1Vwd-XOdriSIGz-Zt_DGWUtM4zkVKJSqQWtpw0iq8lkMwag_jKwkOSLthD-vuB8WKfUGuk8Ic5xRqbgEJAoTqQH6W4OaEvuwjME2QkEBCmt1wGGbvbKWzlngndPZfR_fISZD0Io2PLae0b0rIF_x1tXYM-7Cy0QCiLVv4g2cjTVMN9KD2HFZ-7HwgbKmNJJERDktT678FbcleTw_w_dAJEKYc6NFEVk28VUHrK7vxGM3lwh-kJagIB4HpsfBgwV04FaVGlbNTauS8xalrFa2BT8lvrVP4NasWrm3OeU3m78QDBiy6K4NzK6zpdN12J_jWS2W9JRfWKZVpp4hFpkEO8pI3jzB3e38LuCyE93xPkqxXJ9QDMyNny8guUBNKzBzltnSk7PWPBRN7Qnfq1Ez0KSZDov5zu8uQIdd5mlP4Ocldo"

    try:
        num_workers = 20
        answers = []
        answers_lock = asyncio.Lock()
        df = pl.read_excel("Tests.xlsx")
        questions = df["QUESTION"].to_list()

        start_time = time.time()

        # Create a queue and add all questions
        queue = asyncio.Queue()
        for question in questions:
            await queue.put(question)

        # Create num_workers worker tasks
        workers = []
        for i in range(num_workers):
            worker_task = asyncio.create_task(
                worker(
                    queue, bot_token, crosscutting_token,
                    answers, answers_lock
                )
            )
            workers.append(worker_task)

        # Wait for all questions to be processed
        await queue.join()

        # Stop all workers
        for i in range(num_workers):
            await queue.put(None)  # Send sentinel to stop workers

        # Wait for all workers to finish
        await asyncio.gather(*workers)

        end_time = time.time()
        total_time = end_time - start_time
        print(f"Took {total_time} seconds")
        if answers:
            df_to_save = pl.DataFrame(answers)
            df_to_save.write_excel("results-test-batch.xlsx")
            print("Results saved to results-test-batch.xlsx")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
