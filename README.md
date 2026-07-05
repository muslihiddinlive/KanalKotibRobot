# KanalKotibRobot

Telegram scheduler + auto-reaction bot (multi-user, dinamik kanallar).

## Xususiyatlar
- Inline tugmalar orqali to'liq boshqaruv
- Kanallar STATIK env orqali emas, DINAMIK aniqlanadi: botni istalgan
  kanalga admin qilib qo'shsangiz bo'ldi, u avtomatik ro'yxatga qo'shiladi
- Har bir foydalanuvchi faqat O'ZI admin bo'lgan kanallarga post
  rejalashtira oladi (get_chat_member orqali tekshiriladi)
- Belgilangan vaqtda kanalga xabar (matn/rasm/video/hujjat) yuborish
- Bot admin bo'lgan har bir kanaldagi har qanday postga avtomatik ⚡ reaksiya
- Ma'lumotlar bazasi sifatida alohida Telegram supergroup ishlatiladi
  (Render'ning vaqtinchalik diskiga bog'liq emas)
- Webhook orqali ishlaydi (Render uchun moslashtirilgan)

## ENV o'zgaruvchilar
- `BOT_TOKEN`
- `WEBHOOK_URL`
- `DB_GROUP_ID` - bot admin bo'lgan alohida supergroup (database sifatida)
- `PORT` (ixtiyoriy, default 8080)
- `TIMEZONE` (ixtiyoriy, default Asia/Tashkent)

## Ishga tushirish
```
pip install -r requirements.txt
python bot.py
```
