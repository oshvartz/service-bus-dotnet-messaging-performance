# Stage 1: Build
# Use the .NET SDK image for building the application
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# Copy the solution file and project file first to leverage Docker layer caching.
COPY ["ThroughputTest/ThroughputTest.sln", "ThroughputTest/"]
COPY ["ThroughputTest/ThroughputTest.csproj", "ThroughputTest/"]

# Restore dependencies for the project.
RUN dotnet restore "ThroughputTest/ThroughputTest.csproj"

# Copy the rest of the application's source code.
COPY ["ThroughputTest/", "ThroughputTest/"]

# Publish the application.
WORKDIR "/src/ThroughputTest"
RUN dotnet publish "ThroughputTest.csproj" -c Release -o /app/publish --no-restore /p:UseAppHost=false -f net8.0

# Stage 2: Runtime
# Use the ASP.NET Core runtime image for the final application.
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS final
WORKDIR /app

# Copy the published application artifacts from the build stage.
COPY --from=build /app/publish .

# Define the entry point for the application.
ENTRYPOINT ["dotnet", "ThroughputTest.dll"]