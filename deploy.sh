#!/bin/bash

echo "🚀 Deploying AI Batch Processor with Docker Compose..."

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

# Check if Docker Compose is available
if ! docker compose version &> /dev/null; then
    echo "❌ Docker Compose is not available. Please install Docker Compose v2 or Docker Desktop."
    exit 1
fi

# Create necessary directories
mkdir -p uploads outputs logs ssl

# Build and start the services
echo "📦 Building and starting services..."
docker compose up -d --build

# Wait for services to be ready
echo "⏳ Waiting for services to start..."
sleep 10

# Check service status
echo "📊 Checking service status..."
docker compose ps

# Show logs
echo "📝 Recent logs:"
docker compose logs --tail=20

echo ""
echo "✅ Deployment complete!"
echo "🌐 Access your app at: http://localhost:8000"
echo "📊 API docs at: http://localhost:8000/docs"
echo ""
echo "📋 Useful commands:"
echo "  View logs: docker compose logs -f"
echo "  Stop services: docker compose down"
echo "  Restart services: docker compose restart"
echo "  Update and restart: docker compose up -d --build"
