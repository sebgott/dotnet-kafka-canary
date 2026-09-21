FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS base
USER $APP_UID
WORKDIR /app
EXPOSE 8080
EXPOSE 8081

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG BUILD_CONFIGURATION=Release
# Disable W^X JIT memory protection: it crashes MSBuild with AccessViolationException
# when building linux/amd64 under QEMU emulation (e.g. on Apple Silicon hosts).
ENV DOTNET_EnableWriteXorExecute=0
WORKDIR /src
COPY ["dotnet-kafka-canary.csproj", "./"]
RUN dotnet restore "dotnet-kafka-canary.csproj"
COPY . .
WORKDIR "/src/"
RUN dotnet build "./dotnet-kafka-canary.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./dotnet-kafka-canary.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "dotnet-kafka-canary.dll"]
