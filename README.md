# KanalKotibRobot

Telegram scheduler + auto-reaction bot.

## Xususiyatlar
- Inline tugmalar orqali to'liq boshqaruv (rejalashtirish, ro'yxat, bekor qilish)
- Belgilangan vaqtda kanalga xabar (matn/rasm/video/hujjat) yuborish
- Bot admin bo'lgan har bir kanaldagi har qanday postga (o'zinikimi, boshqasinikimi) avtomatik ⚡ reaksiya
- Ma'lumotlar bazasi sifatida alohida Telegram supergroup ishlatiladi (Render'ning vaqtinchalik diskiga bog'liq emas)
- Webhook orqali ishlaydi (Render uchun moslashtirilgan)

## ENV o'zgaruvchilar
- `BOT_TOKEN`
- `WEBHOOK_URL`
- `CHANNELS` (vergul bilan ajratilgan kanal ID lari)
- `SUPERADMIN_ID`
- `DB_GROUP_ID`
- `PORT` (ixtiyoriy, default 8080)
- `TIMEZONE` (ixtiyoriy, default Asia/Tashkent)

## Ishga tushirish
```
pip install -r requirements.txt
python bot.py
```
