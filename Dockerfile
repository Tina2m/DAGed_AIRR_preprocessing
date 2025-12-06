FROM immcantation/suite:4.6.0

# Install API deps
RUN pip install --no-cache-dir fastapi uvicorn[standard] python-multipart pydantic

WORKDIR /app

# Copy backend and UI
COPY app/ /app/
COPY ui/ /app/ui/

EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
