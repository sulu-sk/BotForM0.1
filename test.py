"""
Telegram Бот для Записи на Приём
Система бронирования для мастеров (барберы, стилисты и т.д.) и клиентов.

Требования:
pip install python-telegram-bot==20.7
"""

import sqlite3
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ConversationHandler,
    ContextTypes,
    filters
)

# ==================== КОНФИГУРАЦИЯ ====================
MASTER_ID = 1025557118  # Замените на ваш Telegram ID
BOT_TOKEN = "8272115600:AAFgmylky-QG3oQNSqw5KcQ2JIzu7N0JmIU"  # Замените на токен вашего бота

# Состояния разговора
CHOOSING_PERIOD, SELECTING_DATES, SELECTING_TIME_SLOTS = range(3)
BOOKING_DATE, BOOKING_TIME, BOOKING_NAME = range(3, 6)

# Константы для форматирования дат
MONTH_NAMES = {
    1: 'января', 2: 'февраля', 3: 'марта', 4: 'апреля',
    5: 'мая', 6: 'июня', 7: 'июля', 8: 'августа',
    9: 'сентября', 10: 'октября', 11: 'ноября', 12: 'декабря'
}

WEEKDAY_NAMES = {
    0: 'Пн', 1: 'Вт', 2: 'Ср', 3: 'Чт', 4: 'Пт', 5: 'Сб', 6: 'Вс'
}

WEEKDAY_FULL = {
    0: 'понедельник', 1: 'вторник', 2: 'среда', 3: 'четверг',
    4: 'пятница', 5: 'суббота', 6: 'воскресенье'
}


# ==================== DATABASE ====================
def init_db():
    """Инициализация базы данных"""
    conn = sqlite3.connect('bookings.db')
    c = conn.cursor()
    
    c.execute('''CREATE TABLE IF NOT EXISTS available_slots
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  date TEXT NOT NULL,
                  time TEXT NOT NULL,
                  is_booked INTEGER DEFAULT 0,
                  UNIQUE(date, time))''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS bookings
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  client_name TEXT NOT NULL,
                  client_username TEXT,
                  client_id INTEGER,
                  date TEXT NOT NULL,
                  time TEXT NOT NULL,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    
    conn.commit()
    conn.close()


def get_db():
    """Получить подключение к базе данных"""
    return sqlite3.connect('bookings.db')


def add_available_slots(dates, times):
    """Добавить доступные временные слоты"""
    conn = get_db()
    c = conn.cursor()
    
    for date in dates:
        for time in times:
            try:
                c.execute('INSERT OR IGNORE INTO available_slots (date, time) VALUES (?, ?)',
                         (date, time))
            except Exception as e:
                print(f"[ERROR] Failed to add slot {date} {time}: {e}")
    
    conn.commit()
    conn.close()


def get_available_dates():
    """Получить все доступные даты со свободными слотами"""
    conn = get_db()
    c = conn.cursor()
    c.execute('''SELECT DISTINCT date FROM available_slots 
                 WHERE is_booked = 0 
                 ORDER BY date''')
    dates = [row[0] for row in c.fetchall()]
    conn.close()
    return dates


def get_available_times(date):
    """Получить доступные временные слоты для конкретной даты"""
    conn = get_db()
    c = conn.cursor()
    c.execute('''SELECT time FROM available_slots 
                 WHERE date = ? AND is_booked = 0 
                 ORDER BY time''', (date,))
    times = [row[0] for row in c.fetchall()]
    conn.close()
    return times


def book_appointment(client_name, client_username, client_id, date, time):
    """Забронировать запись"""
    conn = get_db()
    c = conn.cursor()
    
    try:
        c.execute('''UPDATE available_slots 
                     SET is_booked = 1 
                     WHERE date = ? AND time = ?''', (date, time))
        
        c.execute('''INSERT INTO bookings 
                     (client_name, client_username, client_id, date, time) 
                     VALUES (?, ?, ?, ?, ?)''',
                 (client_name, client_username, client_id, date, time))
        
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"[ERROR] Booking failed: {e}")
        conn.close()
        return False


def get_all_bookings():
    """Получить все записи для CRM"""
    conn = get_db()
    c = conn.cursor()
    c.execute('''SELECT client_name, client_username, date, time, created_at 
                 FROM bookings 
                 ORDER BY date, time''')
    bookings = c.fetchall()
    conn.close()
    return bookings


def cancel_booking(date, time):
    """Отменить запись и освободить слот"""
    conn = get_db()
    c = conn.cursor()
    
    try:
        c.execute('SELECT client_id FROM bookings WHERE date = ? AND time = ?', (date, time))
        result = c.fetchone()
        client_id = result[0] if result else None
        
        c.execute('UPDATE available_slots SET is_booked = 0 WHERE date = ? AND time = ?',
                 (date, time))
        c.execute('DELETE FROM bookings WHERE date = ? AND time = ?', (date, time))
        conn.commit()
        conn.close()
        return True, client_id
    except Exception as e:
        print(f"[ERROR] Cancel booking failed: {e}")
        conn.close()
        return False, None


def delete_date_slots(date):
    """Удалить все слоты и записи для конкретной даты"""
    conn = get_db()
    c = conn.cursor()
    
    try:
        c.execute('SELECT client_name, client_id, time FROM bookings WHERE date = ?', (date,))
        affected_bookings = c.fetchall()
        
        c.execute('DELETE FROM bookings WHERE date = ?', (date,))
        c.execute('DELETE FROM available_slots WHERE date = ?', (date,))
        
        conn.commit()
        conn.close()
        return True, affected_bookings
    except Exception as e:
        print(f"[ERROR] Delete date slots failed: {e}")
        conn.close()
        return False, []


def get_dates_with_slots():
    """Получить все даты, где есть слоты"""
    conn = get_db()
    c = conn.cursor()
    c.execute('''SELECT DISTINCT date FROM available_slots 
                 ORDER BY date''')
    dates = [row[0] for row in c.fetchall()]
    conn.close()
    return dates


# ==================== UTILITIES ====================
def format_date_russian(date_str):
    """Форматировать дату на русском языке (полный формат)"""
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    day = date_obj.day
    month = MONTH_NAMES[date_obj.month]
    year = date_obj.year
    weekday = WEEKDAY_FULL[date_obj.weekday()]
    return f"{day} {month} {year} ({weekday})"


def format_date_short(date_str):
    """Короткий формат даты"""
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    day = date_obj.day
    month = MONTH_NAMES[date_obj.month]
    weekday = WEEKDAY_NAMES[date_obj.weekday()]
    return f"{day} {month} ({weekday})"


def get_master_menu_keyboard():
    """Клавиатура главного меню мастера"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📅 Установить даты", callback_data="master_setdays")],
        [InlineKeyboardButton("📊 Посмотреть записи (CRM)", callback_data="master_crm")],
        [InlineKeyboardButton("❌ Отменить запись", callback_data="master_cancel_booking")],
        [InlineKeyboardButton("🗑 Удалить день", callback_data="master_delete_day")],
    ])


def get_client_menu_keyboard():
    """Клавиатура главного меню клиента"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📅 Записаться на приём", callback_data="client_book")],
        [InlineKeyboardButton("ℹ️ Мои записи", callback_data="client_mybookings")]
    ])


# ==================== COMMAND HANDLERS ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка команды /start"""
    user = update.effective_user
    
    if user.id == MASTER_ID:
        message = "👋 Добро пожаловать, Мастер!\n\nВыберите действие:"
        await update.message.reply_text(message, reply_markup=get_master_menu_keyboard())
    else:
        message = f"👋 Добро пожаловать, {user.first_name}!\n\nВыберите действие:"
        await update.message.reply_text(message, reply_markup=get_client_menu_keyboard())


async def crm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать все записи (только для мастера)"""
    user = update.effective_user
    
    if user.id != MASTER_ID:
        await update.message.reply_text("❌ Эта команда доступна только для мастера.")
        return
    
    bookings = get_all_bookings()
    
    if not bookings:
        await update.message.reply_text("📊 Записей пока нет.")
        return
    
    message = "📊 *Все записи:*\n\n"
    current_date = None
    
    for booking in bookings:
        client_name, client_username, date, time, created_at = booking
        
        if date != current_date:
            display_date = format_date_russian(date)
            message += f"\n📅 *{display_date}*\n"
            current_date = date
        
        username_display = f"@{client_username}" if client_username != "Не указан" else "Не указан"
        message += f"  • {time} — {client_name} ({username_display})\n"
    
    await update.message.reply_text(message, parse_mode='Markdown')


# ==================== MASTER HANDLERS ====================
async def setdays_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начать процесс установки доступных дней"""
    query = update.callback_query if update.callback_query else None
    user = query.from_user if query else update.effective_user
    
    if user.id != MASTER_ID:
        message = "❌ Эта команда доступна только для мастера."
        if query:
            await query.edit_message_text(message)
        else:
            await update.message.reply_text(message)
        return ConversationHandler.END
    
    keyboard = [
        [InlineKeyboardButton("1 Неделя", callback_data="period_7")],
        [InlineKeyboardButton("2 Недели", callback_data="period_14")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]
        
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    message = "📅 Выберите период для установки дат:"
    if query:
        await query.answer()
        await query.edit_message_text(message, reply_markup=reply_markup)
    else:
        await update.message.reply_text(message, reply_markup=reply_markup)
    
    return CHOOSING_PERIOD


async def period_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора периода"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "back_to_menu":
        await query.edit_message_text(
            "👋 Главное меню мастера.\n\nВыберите действие:",
            reply_markup=get_master_menu_keyboard()
        )
        return ConversationHandler.END
    
    try:
        days = int(query.data.split("_")[1])
        
        # Генерация дат
        dates = []
        today = datetime.now()
        for i in range(days):
            date = today + timedelta(days=i)
            dates.append(date.strftime("%Y-%m-%d"))
        
        context.user_data['dates'] = dates
        context.user_data['selected_dates'] = []
        
        # Создание клавиатуры с датами
        keyboard = []
        for i in range(0, len(dates), 2):
            row = []
            for j in range(i, min(i + 2, len(dates))):
                display = format_date_short(dates[j])
                row.append(InlineKeyboardButton(display, callback_data=f"date_{dates[j]}"))
            keyboard.append(row)
        
        keyboard.append([
            InlineKeyboardButton("✅ Готово", callback_data="dates_done"),
            InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")
        ])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            f"📅 Выберите даты ({len(dates)} дней):\n"
            "Нажимайте на даты для выбора, затем нажмите Готово.",
            reply_markup=reply_markup
        )
        
        return SELECTING_DATES
        
    except Exception as e:
        print(f"[ERROR] Error in period_selected: {e}")
        await query.edit_message_text("Произошла ошибка. Попробуйте еще раз.")
        return ConversationHandler.END


async def date_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Переключение выбора даты"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "back_to_menu":
        await query.edit_message_text(
            "👋 Главное меню мастера.\n\nВыберите действие:",
            reply_markup=get_master_menu_keyboard()
        )
        context.user_data.clear()
        return ConversationHandler.END
    
    if query.data == "dates_done":
        if not context.user_data.get('selected_dates'):
            await query.answer("⚠️ Пожалуйста, выберите хотя бы одну дату!", show_alert=True)
            return SELECTING_DATES
        
        # Показать выбор временных слотов
        time_slots = []
        for hour in range(9, 21):
            time_slots.append(f"{hour:02d}:00")
            if hour < 20:
                time_slots.append(f"{hour:02d}:30")
        
        context.user_data['available_times'] = time_slots
        context.user_data['selected_times'] = []
        
        keyboard = []
        for i in range(0, len(time_slots), 4):
            row = []
            for j in range(i, min(i + 4, len(time_slots))):
                row.append(InlineKeyboardButton(
                    time_slots[j], 
                    callback_data=f"timeslot_{time_slots[j]}"
                ))
            keyboard.append(row)
        
        keyboard.append([
            InlineKeyboardButton("✅ Подтвердить выбор", callback_data="times_done"),
            InlineKeyboardButton("🔙 Назад", callback_data="back_to_period")
        ])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            "⏰ Выберите доступное время:\n"
            "Нажимайте на время для выбора.\n"
            "Выбранное время будет отмечено ✅\n\n"
            "Выбрано: 0",
            reply_markup=reply_markup
        )
        return SELECTING_TIME_SLOTS
    
    # Переключить дату
    date = query.data.split("_")[1]
    selected = context.user_data.get('selected_dates', [])
    
    if date in selected:
        selected.remove(date)
    else:
        selected.append(date)
    
    context.user_data['selected_dates'] = selected
    
    # Перестроить клавиатуру
    dates = context.user_data['dates']
    keyboard = []
    for i in range(0, len(dates), 2):
        row = []
        for j in range(i, min(i + 2, len(dates))):
            display = format_date_short(dates[j])
            if dates[j] in selected:
                display = f"✅ {display}"
            row.append(InlineKeyboardButton(display, callback_data=f"date_{dates[j]}"))
        keyboard.append(row)
    
    keyboard.append([
        InlineKeyboardButton("✅ Готово", callback_data="dates_done"),
        InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu")
    ])
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        f"📅 Выберите даты для записи (выбрано: {len(selected)}/{len(dates)}):\n"
        f"Нажимайте на даты для выбора, затем нажмите Готово.",
        reply_markup=reply_markup
    )
    
    return SELECTING_DATES


async def time_slot_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Переключение выбора временного слота"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "times_done":
        selected_times = context.user_data.get('selected_times', [])
        
        if not selected_times:
            await query.answer("⚠️ Пожалуйста, выберите хотя бы один временной слот!", show_alert=True)
            return SELECTING_TIME_SLOTS
        
        # Сохранить в базу данных
        dates = context.user_data['selected_dates']
        add_available_slots(dates, selected_times)
        
        dates_display = ', '.join([format_date_short(d) for d in dates])
        
        await query.edit_message_text(
            f"✅ Успешно добавлено {len(dates)} дат(ы) с {len(selected_times)} временными слотами!\n\n"
            f"Всего создано слотов: {len(dates) * len(selected_times)}\n\n"
            f"Даты: {dates_display}\n"
            f"Время: {', '.join(sorted(selected_times))}"
        )
        
        context.user_data.clear()
        return ConversationHandler.END
    
    if query.data == "back_to_period":
        # Вернуться к выбору дат
        dates = context.user_data['dates']
        keyboard = []
        for i in range(0, len(dates), 2):
            row = []
            for j in range(i, min(i + 2, len(dates))):
                display = format_date_short(dates[j])
                row.append(InlineKeyboardButton(display, callback_data=f"date_{dates[j]}"))
            keyboard.append(row)
        
        keyboard.append([InlineKeyboardButton("✅ Готово", callback_data="dates_done")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        context.user_data['selected_dates'] = []
        
        await query.edit_message_text(
            f"📅 Выберите даты для записи (выбрано: 0/{len(dates)}):\n"
            f"Нажимайте на даты для выбора, затем нажмите Готово.",
            reply_markup=reply_markup
        )
        return SELECTING_DATES
    
    # Переключить временной слот
    time = query.data.split("_")[1]
    selected_times = context.user_data.get('selected_times', [])
    
    if time in selected_times:
        selected_times.remove(time)
    else:
        selected_times.append(time)
    
    context.user_data['selected_times'] = selected_times
    
    # Перестроить клавиатуру
    time_slots = context.user_data['available_times']
    keyboard = []
    
    for i in range(0, len(time_slots), 4):
        row = []
        for j in range(i, min(i + 4, len(time_slots))):
            display = time_slots[j]
            if time_slots[j] in selected_times:
                display = f"✅ {display}"
            row.append(InlineKeyboardButton(
                display, 
                callback_data=f"timeslot_{time_slots[j]}"
            ))
        keyboard.append(row)
    
    keyboard.append([
        InlineKeyboardButton("✅ Подтвердить выбор", callback_data="times_done"),
        InlineKeyboardButton("🔙 Назад", callback_data="back_to_period")
    ])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        "⏰ Выберите доступное время:\n"
        "Нажимайте на время для выбора.\n"
        "Выбранное время будет отмечено ✅\n\n"
        f"Выбрано: {len(selected_times)}",
        reply_markup=reply_markup
    )
    
    return SELECTING_TIME_SLOTS


# ==================== CLIENT BOOKING HANDLERS ====================
async def start_booking(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало процесса записи клиента"""
    query = update.callback_query
    await query.answer()
    
    dates = get_available_dates()
    if not dates:
        await query.edit_message_text(
            "😔 К сожалению, в данный момент нет доступных дат для записи.\n"
            "Пожалуйста, проверьте позже!",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")
            ]])
        )
        return ConversationHandler.END

    keyboard = []
    for i in range(0, len(dates), 2):
        row = []
        for j in range(i, min(i + 2, len(dates))):
            display_date = format_date_short(dates[j])
            row.append(InlineKeyboardButton(display_date, callback_data=f"bookdate_{dates[j]}"))
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")])
    
    await query.edit_message_text(
        "📅 Выберите дату:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return BOOKING_DATE


async def booking_date_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора даты для записи"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "back_to_menu":
        await query.edit_message_text(
            "👋 Главное меню.\n\nВыберите действие:",
            reply_markup=get_client_menu_keyboard()
        )
        context.user_data.clear()
        return ConversationHandler.END
    
    if query.data == "back_to_dates":
        # Вернуться к выбору дат
        dates = get_available_dates()
        keyboard = []
        for i in range(0, len(dates), 2):
            row = []
            for j in range(i, min(i + 2, len(dates))):
                display_date = format_date_short(dates[j])
                row.append(InlineKeyboardButton(display_date, callback_data=f"bookdate_{dates[j]}"))
            keyboard.append(row)
        
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")])
        
        await query.edit_message_text(
            "📅 Выберите дату:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return BOOKING_DATE
    
    # Извлечь дату
    date = query.data.replace("bookdate_", "")
    context.user_data['booking_date'] = date
    
    # Получить доступное время
    times = get_available_times(date)
    
    if not times:
        await query.edit_message_text(
            "😔 К сожалению, все слоты на эту дату уже заняты.\n"
            "Пожалуйста, выберите другую дату.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Назад", callback_data="back_to_dates")
            ]])
        )
        return BOOKING_DATE
    
    # Создать клавиатуру с временными слотами
    keyboard = []
    for i in range(0, len(times), 3):
        row = []
        for j in range(i, min(i + 3, len(times))):
            row.append(InlineKeyboardButton(times[j], callback_data=f"booktime_{times[j]}"))
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_to_dates")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    display_date = format_date_russian(date)
    
    await query.edit_message_text(
        f"⏰ Выберите время для {display_date}:",
        reply_markup=reply_markup
    )
    
    return BOOKING_TIME


async def booking_time_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора времени для записи"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "back_to_dates":
        # Вернуться к выбору дат
        dates = get_available_dates()
        keyboard = []
        for i in range(0, len(dates), 2):
            row = []
            for j in range(i, min(i + 2, len(dates))):
                display_date = format_date_short(dates[j])
                row.append(InlineKeyboardButton(display_date, callback_data=f"bookdate_{dates[j]}"))
            keyboard.append(row)
        
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")])
        
        await query.edit_message_text(
            "📅 Выберите дату:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return BOOKING_DATE
    
    # Извлечь время
    time = query.data.replace("booktime_", "")
    context.user_data['booking_time'] = time
    
    date = context.user_data['booking_date']
    display_date = format_date_russian(date)
    
    await query.edit_message_text(
        f"✅ Выбрано:\n"
        f"📅 Дата: {display_date}\n"
        f"⏰ Время: {time}\n\n"
        f"👤 Пожалуйста, введите ваше полное имя:"
    )
    
    return BOOKING_NAME


async def back_to_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик возврата в меню"""
    query = update.callback_query
    await query.answer()
    
    if query.from_user.id == MASTER_ID:
        await query.edit_message_text(
            "👋 Главное меню мастера",
            reply_markup=get_master_menu_keyboard()
        )
    else:
        await query.edit_message_text(
            "👋 Главное меню",
            reply_markup=get_client_menu_keyboard()
        )
    return ConversationHandler.END

# Fix booking_name_received handler
async def booking_name_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка имени клиента и завершение записи"""
    print("[DEBUG] booking_name_received called")  # Отладочный вывод
    
    try:
        client_name = update.message.text.strip()
        
        if len(client_name) < 2:
            await update.message.reply_text(
                "❌ Имя должно содержать минимум 2 символа.\n"
                "Пожалуйста, введите имя ещё раз:"
            )
            return BOOKING_NAME
        
        # Получаем данные из контекста
        date = context.user_data.get('booking_date')
        time = context.user_data.get('booking_time')
        
        print(f"[DEBUG] Booking details: date={date}, time={time}")  # Отладка
        
        if not date or not time:
            await update.message.reply_text(
                "❌ Ошибка: данные записи не найдены.\n"
                "Пожалуйста, начните запись заново:",
                reply_markup=get_client_menu_keyboard()
            )
            return ConversationHandler.END
        
        # Сохраняем данные пользователя
        user = update.effective_user
        client_username = user.username or "Не указан"
        client_id = user.id
        
        # Пытаемся создать запись
        success = book_appointment(
            client_name=client_name,
            client_username=client_username,
            client_id=client_id,
            date=date,
            time=time
        )
        
        if success:
            # Отправляем подтверждение клиенту
            display_date = format_date_russian(date)
            await update.message.reply_text(
                f"✅ Ваша запись подтверждена!\n\n"
                f"👤 Имя: {client_name}\n"
                f"📅 Дата: {display_date}\n"
                f"⏰ Время: {time}\n\n"
                f"Ждём вас! 😊",
                reply_markup=get_client_menu_keyboard()
            )
            
            # Уведомляем мастера
            try:
                await context.bot.send_message(
                    chat_id=MASTER_ID,
                    text=(
                        f"📝 Новая запись!\n\n"
                        f"👤 Клиент: {client_name}\n"
                        f"🔹 Username: @{client_username}\n"
                        f"📅 Дата: {display_date}\n"
                        f"⏰ Время: {time}"
                    )
                )
            except Exception as e:
                print(f"[ERROR] Не удалось уведомить мастера: {e}")
        else:
            await update.message.reply_text(
                "❌ Не удалось создать запись. Возможно, время уже занято.\n"
                "Пожалуйста, попробуйте выбрать другое время:",
                reply_markup=get_client_menu_keyboard()
            )
    
    except Exception as e:
        print(f"[ERROR] Ошибка в booking_name_received: {e}")
        await update.message.reply_text(
            "❌ Произошла ошибка. Пожалуйста, попробуйте позже.",
            reply_markup=get_client_menu_keyboard()
        )
    
    finally:
        # Очищаем данные
        if context.user_data:
            context.user_data.clear()
    
    return ConversationHandler.END

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ошибок"""
    print(f"[ERROR] Exception while handling an update: {context.error}")

async def handle_master_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопок панели мастера"""
    query = update.callback_query
    await query.answer()

    if query.data == "back_to_menu":
        # Возврат в главное меню
        if query.from_user.id == MASTER_ID:
            await query.edit_message_text(
                "👋 Главное меню мастера",
                reply_markup=get_master_menu_keyboard()
            )
        else:
            await query.edit_message_text(
                "👋 Главное меню",
                reply_markup=get_client_menu_keyboard()
            )
        return

    if query.from_user.id != MASTER_ID:
        await query.edit_message_text("❌ Доступ запрещен")
        return

    if query.data == "master_crm":
        # Показать все записи
        bookings = get_all_bookings()
        if not bookings:
            await query.edit_message_text(
                "📊 Записей пока нет",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")
                ]])
            )
            return

        message = "📊 *Все записи:*\n\n"
        current_date = None
        
        for booking in bookings:
            client_name, client_username, date, time, created_at = booking
            
            if date != current_date:
                display_date = format_date_russian(date)
                message += f"\n📅 *{display_date}*\n"
                current_date = date
            
            username_display = f"@{client_username}" if client_username != "Не указан" else "Не указан"
            message += f"  • {time} — {client_name} ({username_display})\n"

        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        try:
            await query.edit_message_text(
                message,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
        except Exception as e:
            print(f"[ERROR] Ошибка при отправке CRM: {e}")
            # Если сообщение слишком длинное, разделим его
            if len(message) > 4096:
                parts = [message[i:i+4096] for i in range(0, len(message), 4096)]
                for i, part in enumerate(parts):
                    if i == len(parts) - 1:  # Последняя часть
                        await query.edit_message_text(
                            part,
                            reply_markup=reply_markup,
                            parse_mode='Markdown'
                        )
                    else:
                        await context.bot.send_message(
                            chat_id=query.message.chat_id,
                            text=part,
                            parse_mode='Markdown'
                        )
            else:
                await query.edit_message_text(
                    "❌ Ошибка при отображении записей",
                    reply_markup=reply_markup
                )
    """Обработчик кнопок панели мастера"""
    query = update.callback_query
    await query.answer()

    if query.data == "back_to_menu":
        # Возврат в главное меню
        if query.from_user.id == MASTER_ID:
            await query.edit_message_text(
                "👋 Главное меню мастера",
                reply_markup=get_master_menu_keyboard()
            )
        else:
            await query.edit_message_text(
                "👋 Главное меню",
                reply_markup=get_client_menu_keyboard()
            )
        return

    if query.from_user.id != MASTER_ID:
        await query.edit_message_text("❌ Доступ запрещен")
        return
    elif query.data == "master_cancel_booking":
        # Показать даты для отмены записи
        dates = get_dates_with_slots()
        if not dates:
            await query.edit_message_text(
                "❌ Нет активных записей",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")
                ]])
            )
            return

        keyboard = []
        for date in dates:
            display_date = format_date_short(date)
            keyboard.append([
                InlineKeyboardButton(display_date, callback_data=f"cancel_date_{date}")
            ])
        
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            "📅 Выберите дату для отмены записи:",
            reply_markup=reply_markup
        )

    elif query.data == "master_delete_day":
        # Показать даты для удаления
        dates = get_dates_with_slots()
        if not dates:
            await query.edit_message_text(
                "❌ Нет доступных дат для удаления",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")
                ]])
            )
            return

        keyboard = []
        for date in dates:
            display_date = format_date_short(date)
            keyboard.append([
                InlineKeyboardButton(display_date, callback_data=f"delete_day_{date}")
            ])
        
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            "⚠️ Выберите день для удаления\n"
            "(Это действие удалит все записи на выбранную дату):",
            reply_markup=reply_markup
        )
# Исправьте функцию handle_cancel_date:

async def handle_cancel_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик отмены записи на конкретную дату"""
    query = update.callback_query
    await query.answer()

    if query.from_user.id != MASTER_ID:
        await query.edit_message_text("❌ Доступ запрещен")
        return

    try:
        if query.data.startswith("cancel_date_"):
            # Обработка выбора даты
            date = query.data.replace("cancel_date_", "")
            # Получаем все записи на эту дату
            conn = get_db()
            c = conn.cursor()
            c.execute('''
                SELECT time, client_name, client_username 
                FROM bookings 
                WHERE date = ? 
                ORDER BY time
            ''', (date,))
            bookings = c.fetchall()
            conn.close()

            if not bookings:
                await query.edit_message_text(
                    "❌ На эту дату нет записей",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔙 Назад", callback_data="master_cancel_booking")
                    ]])
                )
                return

            # Создаем клавиатуру с записями
            keyboard = []
            for time, client_name, client_username in bookings:
                display_name = f"{client_name} (@{client_username})" if client_username else client_name
                # Изменяем формат callback_data
                callback_data = f"cancel_booking_{date}_{time}_{client_name}"
                keyboard.append([
                    InlineKeyboardButton(
                        f"{time} - {display_name}", 
                        callback_data=callback_data
                    )
                ])
            
            keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="master_cancel_booking")])
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            display_date = format_date_russian(date)
            await query.edit_message_text(
                f"📅 Записи на {display_date}:\n"
                "Выберите запись для отмены:",
                reply_markup=reply_markup
            )

        elif query.data.startswith("cancel_booking_"):
            # Обработка отмены конкретной записи
            parts = query.data.split("_")
            date = parts[2]
            time = parts[3]
            
            success, client_id = cancel_booking(date, time)
            
            if success:
                display_date = format_date_russian(date)
                await query.edit_message_text(
                    f"✅ Запись успешно отменена!\n"
                    f"Дата: {display_date}\n"
                    f"Время: {time}",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔙 В меню", callback_data="back_to_menu")
                    ]])
                )
                
                # Уведомляем клиента если есть client_id
                if client_id:
                    try:
                        await context.bot.send_message(
                            chat_id=client_id,
                            text=f"❌ Ваша запись на {display_date} в {time} была отменена мастером."
                        )
                    except Exception as e:
                        print(f"[ERROR] Не удалось уведомить клиента: {e}")
            else:
                await query.edit_message_text(
                    "❌ Не удалось отменить запись",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔙 Назад", callback_data="master_cancel_booking")
                    ]])
                )
    except Exception as e:
        print(f"[ERROR] Ошибка в handle_cancel_date: {e}")
        await query.edit_message_text(
            "❌ Произошла ошибка. Попробуйте еще раз.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 В меню", callback_data="back_to_menu")
            ]])
        )

# Update conversation handlers in main()
def main():
    """Запуск бота"""
    application = Application.builder().token(BOT_TOKEN).build()
    init_db()
    
    # Master conversation handler
    master_conv = ConversationHandler(
        entry_points=[
            CommandHandler("setdays", setdays_start),
            CallbackQueryHandler(setdays_start, pattern="^master_setdays$")
        ],
        states={
            CHOOSING_PERIOD: [
                CallbackQueryHandler(period_selected, pattern="^period_"),
                CallbackQueryHandler(back_to_menu, pattern="^back_to_menu$")
            ],
            SELECTING_DATES: [
                CallbackQueryHandler(date_toggle, pattern="^date_"),
                CallbackQueryHandler(date_toggle, pattern="^dates_done$"),
                CallbackQueryHandler(back_to_menu, pattern="^back_to_menu$")
            ],
            SELECTING_TIME_SLOTS: [
                CallbackQueryHandler(time_slot_toggle, pattern="^timeslot_"),
                CallbackQueryHandler(time_slot_toggle, pattern="^times_done$"),
                CallbackQueryHandler(back_to_menu, pattern="^back_to_menu$")
            ]
        },
        fallbacks=[CallbackQueryHandler(back_to_menu, pattern="^back_to_menu$")],
        per_chat=True,
        name="master_conversation"
    )

    # Client booking conversation handler
    booking_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(start_booking, pattern="^client_book$")],
        states={
            BOOKING_DATE: [
                CallbackQueryHandler(booking_date_selected, pattern="^bookdate_"),
                CallbackQueryHandler(back_to_menu, pattern="^back_to_menu$")
            ],
            BOOKING_TIME: [
                CallbackQueryHandler(booking_time_selected, pattern="^booktime_"),
                CallbackQueryHandler(booking_date_selected, pattern="^back_to_dates$"),
                CallbackQueryHandler(back_to_menu, pattern="^back_to_menu$")
            ],
            BOOKING_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, booking_name_received),
                CallbackQueryHandler(back_to_menu, pattern="^back_to_menu$")
            ]
        },
        fallbacks=[CallbackQueryHandler(back_to_menu, pattern="^back_to_menu$")],
        per_chat=True,
        name="booking_conversation"
    )
    # Добавляем обработчик кнопок мастера
    application.add_handler(CallbackQueryHandler(
        handle_master_buttons,
        pattern="^(master_crm|master_cancel_booking|master_delete_day|back_to_menu)$"
    ))
    
    # Добавляем обработчик отмены записей
    application.add_handler(CallbackQueryHandler(
        handle_cancel_date,
        pattern="^(cancel_date_|cancel_booking_).*$"
    ))
    # Регистрация обработчиков
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("crm", crm))
    application.add_handler(master_conv)
    application.add_handler(booking_conv)
    
    # Добавляем обработчик кнопок мастера
    application.add_handler(CallbackQueryHandler(
        handle_master_buttons,
        pattern="^(master_crm|master_cancel_booking|master_delete_day|back_to_menu)$"
    ))

    print("[INFO] Бот запущен!")
    application.run_polling()

if __name__ == "__main__":
    main()

