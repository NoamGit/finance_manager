import asyncio
from functools import partial

import pandas as pd
from prefect import task
from prefect.blocks.system import JSON
from telegram import Bot
from telegram.constants import ParseMode
from telegram.helpers import escape_markdown
from aiogram import Dispatcher, types
from aiogram import Bot as aiogramBot
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from aiogram.types import ParseMode

from src.core.notification.categorization import HighLevelCategories


def build_status_msg(data: pd.DataFrame) -> str:
    res = f"Status - {data['datetime'].iloc[0]}\n\n"
    res += build_progress_bar_msg(data['month_to_date_expense'].sum(), data['limit'].sum(), 'סה"כ')
    return res


def build_progress_bar_msg(expense: float, limit: float, category: str) -> str:
    progress = expense / limit
    progress_bar = '█' * int(progress * 20) + '░' * (20 - int(progress * 20))
    return f"{category}" \
           f"\n\n{progress_bar}" \
           f"\n\n{expense:.1f} ₪ / {limit:.1f} ₪ ({progress * 100:.1f}%)"


async def action_list(message: types.Message):
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(
        types.InlineKeyboardButton(str(HighLevelCategories.VARIABLE_EXPENSE_MUTUAL.value),
                                   callback_data=str(HighLevelCategories.VARIABLE_EXPENSE_MUTUAL.value)))
    keyboard.add(types.InlineKeyboardButton(str(HighLevelCategories.VARIABLE_EXPENSE_NOAM.value),
                                            callback_data=str(HighLevelCategories.VARIABLE_EXPENSE_NOAM.value)))
    keyboard.add(types.InlineKeyboardButton(str(HighLevelCategories.VARIABLE_EXPENSE_EDEN.value),
                                            callback_data=str(HighLevelCategories.VARIABLE_EXPENSE_EDEN.value)))
    keyboard.add(types.InlineKeyboardButton(str(HighLevelCategories.FIXED.value),
                                            callback_data=str(HighLevelCategories.FIXED.value)))
    keyboard.add(types.InlineKeyboardButton(str(HighLevelCategories.GROCERIES.value),
                                            callback_data=str(HighLevelCategories.GROCERIES.value)))
    keyboard.add(types.InlineKeyboardButton(str(HighLevelCategories.TRANSPORTATION.value),
                                            callback_data=str(HighLevelCategories.TRANSPORTATION.value)))
    await message.reply("בחר פירוט:", reply_markup=keyboard)


async def show_expense_table(query: types.CallbackQuery, data: pd.DataFrame):
    pick = query.data
    await query.answer()
    header = f" פירוט{pick}\n"
    content = header
    filtered_df = data[data['high_category'] == pick]
    if filtered_df.empty:
        content = f"לא נמצאו פרטים עבור {pick}"
    else:
        for r in filtered_df.iterrows():
            row = r[1]
            show_date = row['date'].strftime('%d/%m')
            content += f"\n - {show_date}\t <b>{row['description']}</b> \t\t{row['charged']:.1f}₪"

    await query.message.edit_text(text=escape_markdown(f"{content}"), parse_mode=ParseMode.HTML)


@task()
async def send_monthly_progress_to_telegram(data: pd.DataFrame) -> bool:
    telegram_block = await JSON.load("telegram-credentials")
    bot = Bot(token=telegram_block.value.get('TELEGRAM_BOT_TOKEN'))
    status_msg = build_status_msg(data)
    await bot.send_message(chat_id=telegram_block.value.get('TELEGRAM_CHANNEL_ID')
                           , text=escape_markdown(status_msg, version=2)
                           , parse_mode=ParseMode.MARKDOWN_V2)
    for index, record in data.iterrows():
        progress_text = build_progress_bar_msg(expense=record['month_to_date_expense'], limit=record['limit'],
                                               category=record['high_category'])
        await bot.send_message(chat_id=telegram_block.value.get('TELEGRAM_CHANNEL_ID')
                               , text=escape_markdown(progress_text, version=2)
                               , parse_mode=ParseMode.MARKDOWN_V2
                               , read_timeout=30
                               , pool_timeout=30)
    return True


@task()
async def enrich_with_more_details(data: pd.DataFrame, timeout: int = 30 * 1):
    if data.empty:
        return

    telegram_block = await JSON.load("telegram-credentials")
    token = telegram_block.value.get('TELEGRAM_BOT_TOKEN')
    bot = aiogramBot(token=token, parse_mode=types.ParseMode.HTML, timeout=timeout)
    dp = Dispatcher(bot)
    dp.middleware.setup(LoggingMiddleware())

    dp.register_message_handler(action_list, commands=["d"])
    partial_button_callback = partial(show_expense_table, data=data)
    dp.register_callback_query_handler(partial_button_callback)

    polling_task = asyncio.create_task(dp.start_polling())
    _, pending = await asyncio.wait({polling_task}, timeout=timeout)
    for task in pending:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    await dp.storage.close()
    await dp.storage.wait_closed()
    await bot.session.close()  # Close the aiohttp.ClientSession
    await bot.close()
