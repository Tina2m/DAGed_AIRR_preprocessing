FROM immcantation/suite:4.6.0

# Install API and database deps
RUN pip install --no-cache-dir \
    fastapi \
    uvicorn[standard] \
    python-multipart \
    pydantic \
    sqlalchemy \
    psycopg2-binary

WORKDIR /app

# Copy full project so app.* imports resolve (app.main, app.config, etc.)
COPY app/ /app/app/
COPY ui/ /app/ui/
COPY scripts/ /app/scripts/

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
