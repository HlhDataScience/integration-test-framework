import json
import time
import asyncio
import aiohttp  # type: ignore
import websockets  # type: ignore
import polars as pl  # type: ignore
from datetime import datetime, UTC  # type: ignore
from websockets.asyncio.client import connect  # type: ignore
from src.configmodels.config_types import pydantic_config, pydantic_settings_config

class DirectLineBotClient:
    """
    Asynchronous client for interacting with a Microsoft Bot Framework bot via Direct Line.

    This class provides functionality to:
    - Request and manage Direct Line tokens
    - Start conversations with the bot
    - Send user messages with contextual metadata
    - Listen for and process bot responses over WebSocket
    - Run multiple workers concurrently to handle batch question processing
    - Save results into an Excel file using Polars

    Attributes
    ----------
    configuration : pydantic_config
        Configuration object providing bot service URLs.
    config_env : pydantic_settings_config
        Environment configuration containing tokens and environment-specific settings.
    """

    def __init__(
            self,
            configuration: pydantic_config,
            config_env: pydantic_settings_config
    ):
        """
        Initialize a DirectLineBotClient instance.

        Parameters
        ----------
        configuration : pydantic_config
            Configuration object with Direct Line bot URL.
        config_env : pydantic_settings_config
            Environment configuration containing authentication tokens.
        """
        self.configuration = configuration
        self.config_env = config_env

    async def get_direct_line_token(self) -> str:
        """
        Get a Direct Line token using the bot token.

        Returns
        -------
        str
            The generated Direct Line token.

        Raises
        ------
        Exception
            If the request to generate a Direct Line token fails.
        """
        url = self.configuration.direct_line_bot_url
        headers = {
            "Authorization": self.config_env.BOT_TOKEN,
            "Content-Type": "application/json"
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("token")
                raise Exception(f"Failed to get DirectLine token: {response.status}")

    async def start_conversation(self) -> dict:
        """
        Start a new conversation with the bot.

        Returns
        -------
        dict
            The JSON payload returned from the Direct Line API, containing
            the conversation ID and stream URL.

        Raises
        ------
        Exception
            If the request to start a conversation fails.
        """
        url = "https://directline.botframework.com/v3/directline/conversations"
        headers = {
            "Authorization": await self.get_direct_line_token(),
            "Content-Type": "application/json"
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers) as response:
                if response.status == 201:
                    return await response.json()
                raise Exception(f"Failed to start conversation: {response.status}")

    async def _send_message(self, conversation_id: str, text: str, directline_token: str) -> dict:
        """
        Send a message to the bot within an active conversation.

        Parameters
        ----------
        conversation_id : str
            The ID of the ongoing conversation.
        text : str
            The message text to send to the bot.
        directline_token : str
            The Direct Line token for authentication.

        Returns
        -------
        dict
            The JSON response from the bot service.

        Raises
        ------
        Exception
            If the request to send a message fails.
        """
        url = f"https://directline.botframework.com/v3/directline/conversations/{conversation_id}/activities"
        headers = {
            "Authorization": directline_token,
            "Content-Type": "application/json"
        }

        activity = {
            "type": "message",
            "from": {"id": "15864749", "name": "15864749", "role": "user"},
            "channelId": "directline",
            "channelData": {
                "x-unir-crosscutting-token": self.config_env.CROSSCUTTING_TOKEN,
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
            "conversation": {"id": conversation_id},
            "timestamp": datetime.now(UTC).isoformat() + "Z"
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=activity) as response:
                if response.status == 200:
                    return await response.json()
                error_text = await response.text()
                raise Exception(f"Failed to send message: {response.status} - {error_text}")

    @staticmethod
    async def listen_for_messages(websocket, question: str, timeout: int) -> list[dict]:
        """
        Listen for incoming messages from the WebSocket until a response is received
        or the timeout expires.

        Parameters
        ----------
        websocket : websockets.WebSocketClientProtocol
            The WebSocket connection to listen on.
        question : str
            The question text associated with the expected response.
        timeout : int
            The maximum time in seconds to wait for a response.

        Returns
        -------
        list[dict]
            A list of answer dictionaries containing the question and response text.
        """
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
                except json.JSONDecodeError as e:
                    print(f"Failed to decode JSON for question {question}: {e}")
                    continue
                except UnicodeDecodeError as e:
                    print(f"Encoding error: {e}")
                    continue

                if "activities" in data:
                    for activity in data["activities"]:
                        if activity.get("type") == "message":
                            message_text = activity.get("text", "")
                            sender_role = activity.get("from", {}).get("role", "unknown")
                            if sender_role != "user":
                                answers.append({"question": question, "answer": message_text})
                                over = True
        except websockets.exceptions.ConnectionClosed:
            print("WebSocket connection closed")
        except Exception as e:
            print(f"Error listening for messages: {e}")

        return answers

    async def process_question(self, question: str, answers: list, answers_lock: asyncio.Lock):
        """
        Process a single question: send it to the bot, listen for the response,
        and append the result to the answers list.

        Parameters
        ----------
        question : str
            The user question to send.
        answers : list
            A shared list of answers across workers.
        answers_lock : asyncio.Lock
            A lock to synchronize access to the answers list.
        """
        try:
            directline_token = await self.get_direct_line_token()
            conversation = await self.start_conversation()

            async with connect(conversation['streamUrl']) as websocket:
                await self._send_message(conversation['conversationId'], question, directline_token)
                question_answers = await self.listen_for_messages(websocket, question, timeout=120)
                async with answers_lock:
                    answers.extend(question_answers)
        except Exception as e:
            print(f"Error processing question '{question}': {e}")

    async def worker(self, queue: asyncio.Queue, answers: list, answers_lock: asyncio.Lock):
        """
        Worker coroutine that processes questions from the queue until a sentinel
        value (None) is received.

        Parameters
        ----------
        queue : asyncio.Queue
            Queue containing questions to be processed.
        answers : list
            Shared list of answers to store results.
        answers_lock : asyncio.Lock
            Lock to ensure thread-safe writes to the answers list.
        """
        while True:
            question = await queue.get()
            if question is None:
                queue.task_done()
                break
            try:
                await self.process_question(question, answers, answers_lock)
                queue.task_done()
            except Exception as e:
                print(f"Worker error: {e}")
                queue.task_done()

    @staticmethod
    def _df_save(answers: list[dict], output_path: str) -> None:
        """
        Save answers to an Excel file.

        Parameters
        ----------
        answers : list[dict]
            List of answers with question/answer mappings.
        output_path : str
            File path where the Excel file will be saved.

        Raises
        ------
        ValueError
            If the answers list is empty or None.
        """
        if answers is None:
            raise ValueError("No answers, please try again")
        df_to_save = pl.DataFrame(answers)
        df_to_save.write_excel(output_path)
        print(f"Results saved to {output_path}")

    async def workers_processing(
            self,
            input_path: str,
            output_path: str,
            question_column: str,
            n_workers: int = 20
    ) -> None:
        """
        Process a batch of questions from an Excel file using multiple workers.

        Parameters
        ----------
        input_path : str
            Path to the Excel file containing questions.
        output_path : str
            Path where the result Excel file should be saved.
        question_column : str
            The column name in the Excel file that contains the questions.
        n_workers : int, optional
            The number of concurrent worker tasks (default is 20).
        """
        try:
            answers: list = []
            answers_lock = asyncio.Lock()
            df = pl.read_excel(input_path)
            questions = df[question_column].to_list()

            start_time = time.time()
            queue: asyncio.Queue = asyncio.Queue()

            for question in questions:
                await queue.put(item=question)

            workers = [
                asyncio.create_task(self.worker(queue, answers, answers_lock))
                for _ in range(n_workers)
            ]

            await queue.join()

            for _ in range(n_workers):
                await queue.put(item=None)

            await asyncio.gather(*workers)
            total_time = time.time() - start_time
            print(f"Took {total_time} seconds")

            if answers:
                self._df_save(answers, output_path)
        except Exception as e:
            print(f"Error: {e}")


async def main():
    # Bot token and configuration
    bot_token = "dGFpQSOk5VA.UUEkyKPpQ8e8w6X6xG6WAj660AIsr8zc7yjHO56LNws"
    crosscutting_token = "13DQpoa0MWw4Z-WQpJgNB6FEB6x4YV1WX3WFZE1YgAf6ErxZgCmevCzzGwrer0oxAutwgyOsBNBNJ10eEcZroZqXLbKpxw70ukRtns1BtQxPc4mNmJS5aczEzRr1k1Vwd-XOdriSIGz-Zt_DGWUtM4zkVKJSqQWtpw0iq8lkMwag_jKwkOSLthD-vuB8WKfUGuk8Ic5xRqbgEJAoTqQH6W4OaEvuwjME2QkEBCmt1wGGbvbKWzlngndPZfR_fISZD0Io2PLae0b0rIF_x1tXYM-7Cy0QCiLVv4g2cjTVMN9KD2HFZ-7HwgbKmNJJERDktT678FbcleTw_w_dAJEKYc6NFEVk28VUHrK7vxGM3lwh-kJagIB4HpsfBgwV04FaVGlbNTauS8xalrFa2BT8lvrVP4NasWrm3OeU3m78QDBiy6K4NzK6zpdN12J_jWS2W9JRfWKZVpp4hFpkEO8pI3jzB3e38LuCyE93xPkqxXJ9QDMyNny8guUBNKzBzltnSk7PWPBRN7Qnfq1Ez0KSZDov5zu8uQIdd5mlP4Ocldo"

    try:
        direct_line_client = DirectLineBotClient(
            configuration="",
            config_env=""
        )

        await direct_line_client.workers_processing(
            input_path="",
            output_path="",
            question_column="",
            n_workers=20

        )

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
