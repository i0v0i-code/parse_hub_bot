"""Only replies to an active owner-private SMS prompt are consumed."""
from pyrogram import Client,filters
from services.login_verification import handle_reply

@Client.on_message(filters.private & filters.text & filters.reply,group=-20)
async def verification_reply(cli,msg):
    if await handle_reply(cli,msg):
        msg.stop_propagation()
