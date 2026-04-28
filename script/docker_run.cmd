@echo off
setlocal
pushd "%~dp0.."

docker build -t login_identity_pipeline_job . || goto :fail

set RUN_DATE=%~1
if "%RUN_DATE%"=="" for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd"') do set RUN_DATE=%%i

echo Running login_identity_pipeline_job --date %RUN_DATE%
docker run --rm --add-host=host.docker.internal:host-gateway --env-file .env login_identity_pipeline_job --date %RUN_DATE% || goto :fail

popd
endlocal & exit /b 0

:fail
popd
endlocal & exit /b 1