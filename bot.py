import asyncio
import aiosqlite
import os
import logging
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from datetime import datetime
from aiohttp import web

# ========== НАСТРОЙКИ ==========
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TOKEN = "8685393043:AAFPt-30hLGUuICaiGsk4XSELMQkcFzYZKs"
ADMIN_ID = 5990018779
ADMIN_USERNAME = "@Timothy_Oliphant"

PORT = int(os.environ.get("PORT", 8080))

bot = Bot(token=TOKEN, validate_token=False)
dp = Dispatcher()

# ========== ВЕБ-СЕРВЕР ДЛЯ RENDER ==========
async def health_check(request):
    return web.Response(text="OK")

async def start_web_server():
    app = web.Application()
    app.router.add_get("/", health_check)
    app.router.add_get("/health", health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info(f"✅ Веб-сервер запущен на порту {PORT}")

# ========== БАЗА ДАННЫХ ==========
async def init_db():
    async with aiosqlite.connect("users.db") as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT DEFAULT NULL,
                received INTEGER DEFAULT 0,
                bonus_extra INTEGER DEFAULT 0,
                invited_by INTEGER DEFAULT NULL,
                referral_count INTEGER DEFAULT 0,
                join_date TEXT DEFAULT NULL,
                last_bonus_reward INTEGER DEFAULT 0,
                last_week_reward INTEGER DEFAULT 0
            )
        """)
        await db.commit()

async def is_valid_user(user):
    if user.is_bot:
        return False, "Боты не учитываются"
    return True, "OK"

def calculate_rewards(referral_count, last_bonus_reward, last_week_reward):
    bonus_count = 0
    week_count = 0
    target_bonus = (referral_count // 5) * 5
    if target_bonus > last_bonus_reward:
        bonus_count = (target_bonus - last_bonus_reward) // 5
        last_bonus_reward = target_bonus
    target_week = (referral_count // 20) * 20
    if target_week > last_week_reward:
        week_count = (target_week - last_week_reward) // 20
        last_week_reward = target_week
    return bonus_count, week_count, last_bonus_reward, last_week_reward

# ========== КОМАНДА /start ==========
@dp.message(Command("start"))
async def start(message: types.Message):
    user_id = message.from_user.id
    username = message.from_user.username or message.from_user.full_name
    referrer_id = None

    is_valid, reason = await is_valid_user(message.from_user)
    if not is_valid:
        await message.answer(f"❌ {reason}")
        return

    if message.text and " " in message.text:
        parts = message.text.split()
        if len(parts) > 1 and parts[1].startswith("ref_"):
            try:
                referrer_id = int(parts[1].replace("ref_", ""))
                print(f"🔗 Юзер {user_id} пришёл по рефералке от {referrer_id}")
            except:
                pass

    async with aiosqlite.connect("users.db") as db:
        cursor = await db.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
        exists = await cursor.fetchone()

        if not exists:
            await db.execute(
                "INSERT INTO users (user_id, username, received, bonus_extra, invited_by, referral_count, join_date, last_bonus_reward, last_week_reward) VALUES (?, ?, 0, 0, ?, 0, ?, 0, 0)",
                (user_id, username, referrer_id, datetime.now().isoformat())
            )
            await db.commit()

            if referrer_id and referrer_id != user_id:
                cursor = await db.execute(
                    "SELECT referral_count, bonus_extra, last_bonus_reward, last_week_reward FROM users WHERE user_id = ?",
                    (referrer_id,)
                )
                referrer = await cursor.fetchone()

                if referrer:
                    new_count = referrer[0] + 1
                    await db.execute("UPDATE users SET referral_count = ? WHERE user_id = ?", (new_count, referrer_id))
                    await db.commit()

                    bonus_count, week_count, new_last_bonus, new_last_week = calculate_rewards(
                        new_count, referrer[2], referrer[3]
                    )
                    
                    rewards_text = []
                    
                    if bonus_count > 0:
                        await db.execute(
                            "UPDATE users SET bonus_extra = bonus_extra + ?, last_bonus_reward = ? WHERE user_id = ?",
                            (bonus_count, new_last_bonus, referrer_id)
                        )
                        await db.commit()
                        rewards_text.append(f"🎁 +{bonus_count} бонусных сделок")
                    
                    if week_count > 0:
                        await db.execute(
                            "UPDATE users SET last_week_reward = ? WHERE user_id = ?",
                            (new_last_week, referrer_id)
                        )
                        await db.commit()
                        rewards_text.append(f"🔒 {week_count} недель в приватном канале")
                        
                        try:
                            await bot.send_message(
                                ADMIN_ID,
                                f"🏆 Пользователь достиг {new_count} рефералов!\n\n👤 ID: {referrer_id}\n👥 Рефералов: {new_count}\n🎁 Награда: {', '.join(rewards_text)}"
                            )
                        except:
                            pass
                    
                    if rewards_text:
                        msg = f"🎉 Поздравляем! Вы пригласили {new_count} друзей!\n\n🏆 Ваши награды:\n"
                        for reward in rewards_text:
                            msg += f"• {reward}\n"
                        if week_count > 0:
                            msg += f"\n📩 Напишите админу: {ADMIN_USERNAME}\n👆 Он добавит вас в приватный канал."
                        try:
                            await bot.send_message(referrer_id, msg)
                        except:
                            pass

        cursor = await db.execute(
            "SELECT received, bonus_extra, referral_count FROM users WHERE user_id = ?",
            (user_id,)
        )
        result = await cursor.fetchone()
        received = result[0] if result else 0
        bonus_extra = result[1] if result else 0
        referral_count = result[2] if result else 0

        available = (1 if received == 0 else 0) + bonus_extra

        bot_info = await bot.get_me()
        ref_link = f"https://t.me/{bot_info.username}?start=ref_{user_id}"

        if available == 1:
            bonus_text = f"🎁 У тебя есть {available} бесплатный сигнал!\nОн придёт с ближайшей рассылкой.\n\n"
        elif available > 1:
            bonus_text = f"🎁 У тебя есть {available} бесплатных сигнала!\nОни придут с ближайшими рассылками.\n\n"
        else:
            bonus_text = "❌ У тебя нет доступных сигналов.\nПригласи друзей, чтобы получить новые!\n\n"

        await message.answer(
            f"🌟 Приветствую в пробном боте! 🌟\n\n"
            f"📈 О чём бот:\nСигналы по фьючерсам на Bybit 🚀\n\n"
            f"{bonus_text}"
            f"👥 Реферальная система:\nТвоих рефералов: {referral_count}\n• Пригласи 5 друзей → +1 бесплатный сигнал\n• Пригласи 20 друзей → 7 дней в приватном канале\n\n"
            f"🔗 Твоя реферальная ссылка:\n{ref_link}\n\n"
            f"📌 Как получить сигналы:\n1️⃣ Нажми /start — ты в базе\n2️⃣ Жди мою рассылку\n3️⃣ За приглашения получай награды!\n\n"
            f"🔒 Платные сигналы (приватный канал):\n💰 Стоимость: 6$ / 500₽ в неделю\n📩 Для доступа напиши админу: {ADMIN_USERNAME}"
        )

# ========== КОМАНДА /send ==========
@dp.message(Command("send"))
async def send_messages(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return await message.answer("Нет доступа")
    text = message.text.replace("/send ", "")
    async with aiosqlite.connect("users.db") as db:
        async with db.execute("SELECT user_id FROM users WHERE received = 0 OR bonus_extra > 0") as cursor:
            users = await cursor.fetchall()
        count = 0
        for (user_id,) in users:
            try:
                await bot.send_message(user_id, text)
                cursor = await db.execute("SELECT received FROM users WHERE user_id = ?", (user_id,))
                result = await cursor.fetchone()
                if result and result[0] == 0:
                    await db.execute("UPDATE users SET received = 1 WHERE user_id = ?", (user_id,))
                else:
                    await db.execute("UPDATE users SET bonus_extra = bonus_extra - 1 WHERE user_id = ? AND bonus_extra > 0", (user_id,))
                await db.commit()
                count += 1
                await asyncio.sleep(0.05)
            except Exception as e:
                print(f"Ошибка {user_id}: {e}")
    await message.answer(f"✅ Рассылка выполнена! Получили сигнал: {count} пользователей")

# ========== КОМАНДА /broadcast ==========
@dp.message(Command("broadcast"))
async def broadcast_all(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    text = message.text.replace("/broadcast ", "")
    async with aiosqlite.connect("users.db") as db:
        cursor = await db.execute("SELECT user_id FROM users")
        users = await cursor.fetchall()
        count = 0
        for (user_id,) in users:
            try:
                await bot.send_message(user_id, text)
                count += 1
                await asyncio.sleep(0.05)
            except:
                pass
    await message.answer(f"✅ Рассылка объявлений выполнена! Отправлено: {count} пользователям")

# ========== КОМАНДА /stats ==========
@dp.message(Command("stats"))
async def show_stats(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    async with aiosqlite.connect("users.db") as db:
        cursor = await db.execute("SELECT COUNT(*) FROM users")
        total = (await cursor.fetchone())[0]
        cursor = await db.execute("SELECT COUNT(*) FROM users WHERE received = 0")
        not_received = (await cursor.fetchone())[0]
        cursor = await db.execute("SELECT SUM(bonus_extra) FROM users")
        extra_sum = (await cursor.fetchone())[0] or 0
        cursor = await db.execute("SELECT COUNT(*) FROM users WHERE invited_by IS NOT NULL")
        invited = (await cursor.fetchone())[0]
        cursor = await db.execute("SELECT COUNT(*) FROM users WHERE referral_count >= 5")
        five = (await cursor.fetchone())[0]
        cursor = await db.execute("SELECT COUNT(*) FROM users WHERE referral_count >= 20")
        twenty = (await cursor.fetchone())[0]
    await message.answer(
        f"📊 Статистика бота\n\n"
        f"👥 Всего пользователей: {total}\n"
        f"🎁 Ждут основной бонус: {not_received}\n"
        f"✨ Накоплено реферальных бонусов: {extra_sum}\n"
        f"🔗 Пришло по рефералкам: {invited}\n"
        f"🏆 Имеют 5+ рефералов: {five}\n"
        f"🔒 Имеют 20+ рефералов: {twenty}"
    )

# ========== КОМАНДА /all_users ==========
@dp.message(Command("all_users"))
async def show_all_users(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    async with aiosqlite.connect("users.db") as db:
        cursor = await db.execute("SELECT user_id, username, referral_count, bonus_extra, received FROM users ORDER BY user_id")
        users = await cursor.fetchall()
        if not users:
            await message.answer("Нет пользователей")
            return
        text = "👥 Список всех пользователей:\n\n"
        for user_id, username, referral_count, bonus_extra, received in users:
            bonus_total = (1 if received == 0 else 0) + bonus_extra
            text += f"🆔 ID: {user_id}\n📝 Имя: {username}\n👥 Рефералов: {referral_count}\n🎁 Бонусов: {bonus_total}\n━━━━━━━━━━━━━━━━━━\n"
        if len(text) > 4000:
            text = text[:4000] + "\n\n... (обрезано)"
        await message.answer(text)

# ========== КОМАНДА /twenty ==========
@dp.message(Command("twenty"))
async def show_twenty_referrals(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    async with aiosqlite.connect("users.db") as db:
        cursor = await db.execute("SELECT user_id, username, referral_count FROM users WHERE referral_count >= 20 ORDER BY referral_count DESC")
        users = await cursor.fetchall()
        if not users:
            await message.answer("Нет пользователей с 20+ рефералами")
            return
        text = "🔒 Пользователи с 20+ рефералов:\n\n"
        for user_id, username, count in users:
            weeks = count // 20
            text += f"🆔 ID: {user_id}\n📝 Имя: {username}\n👥 Рефералов: {count}\n⏰ Недель: {weeks}\n━━━━━━━━━━━━━━━━━━\n"
        await message.answer(text)

# ========== КОМАНДА /add_bonus ==========
@dp.message(Command("add_bonus"))
async def add_bonus(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    parts = message.text.split()
    if len(parts) != 3:
        await message.answer("❌ Формат: /add_bonus USER_ID КОЛИЧЕСТВО")
        return
    try:
        user_id = int(parts[1])
        amount = int(parts[2])
        if amount <= 0:
            await message.answer("❌ Количество должно быть больше 0")
            return
        async with aiosqlite.connect("users.db") as db:
            cursor = await db.execute("SELECT user_id, username FROM users WHERE user_id = ?", (user_id,))
            user = await cursor.fetchone()
            if not user:
                await message.answer(f"❌ Пользователь {user_id} не найден")
                return
            await db.execute("UPDATE users SET bonus_extra = bonus_extra + ? WHERE user_id = ?", (amount, user_id))
            await db.commit()
        await message.answer(f"✅ Добавлено {amount} бонусов пользователю {user_id} ({user[1]})")
        try:
            await bot.send_message(user_id, f"🎁 Админ добавил вам {amount} бонусных сигналов!")
        except:
            pass
    except:
        await message.answer("❌ Ошибка: ID и количество должны быть числами")

# ========== КОМАНДА /remove_bonus ==========
@dp.message(Command("remove_bonus"))
async def remove_bonus(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    parts = message.text.split()
    if len(parts) != 3:
        await message.answer("❌ Формат: /remove_bonus USER_ID КОЛИЧЕСТВО")
        return
    try:
        user_id = int(parts[1])
        amount = int(parts[2])
        if amount <= 0:
            await message.answer("❌ Количество должно быть больше 0")
            return
        async with aiosqlite.connect("users.db") as db:
            cursor = await db.execute("SELECT user_id, username, bonus_extra FROM users WHERE user_id = ?", (user_id,))
            user = await cursor.fetchone()
            if not user:
                await message.answer(f"❌ Пользователь {user_id} не найден")
                return
            new_amount = max(0, user[2] - amount)
            await db.execute("UPDATE users SET bonus_extra = ? WHERE user_id = ?", (new_amount, user_id))
            await db.commit()
        await message.answer(f"✅ Убрано {amount} бонусов у {user_id} ({user[1]}). Осталось: {new_amount}")
        try:
            await bot.send_message(user_id, f"⚠️ Админ убрал {amount} бонусных сигналов. Осталось: {new_amount}")
        except:
            pass
    except:
        await message.answer("❌ Ошибка")

# ========== КОМАНДА /backup ==========
@dp.message(Command("backup"))
async def backup_db(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    import shutil
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"users_backup_{timestamp}.db"
        if os.path.exists("users.db"):
            shutil.copy2("users.db", backup_name)
            await message.answer(f"✅ Резервная копия создана: {backup_name}")
        else:
            await message.answer("❌ Файл users.db не найден")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")

# ========== КОМАНДА /help_admin ==========
@dp.message(Command("help_admin"))
async def help_admin(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    await message.answer(
        "📋 Список админ-команд:\n\n"
        "📤 /send текст — рассылка с бонусами\n"
        "📢 /broadcast текст — рассылка всем\n"
        "👥 /stats — статистика\n"
        "📋 /all_users — список пользователей\n"
        "🏆 /twenty — 20+ рефералов\n"
        "➕ /add_bonus ID кол-во — добавить бонусы\n"
        "➖ /remove_bonus ID кол-во — убрать бонусы\n"
        "💾 /backup — резервная копия\n"
        "❓ /help_admin — это сообщение"
    )

# ========== ЗАПУСК ==========
async def main():
    await init_db()
    await start_web_server()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())