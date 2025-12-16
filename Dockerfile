FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# Create non-root user
RUN useradd -m -u 1000 planka && chown -R planka:planka /app
USER planka

# Run the bot
CMD ["python", "planka_bot.py"]