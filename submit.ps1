# 如果跑不了在管理员终端中执行 Set-ExecutionPolicy Unrestricted -Scope CurrentUser -Force
$folderPath = ".\src"
$zipPath = ".\submit.zip"
$cmakeFilePath = ".\demos\cpp\CMakeLists.txt"

if (Test-Path $zipPath) {
    Remove-Item $zipPath -Force
}

$cmakeFileInFolder = Test-Path "$folderPath\CMakeLists.txt"

$fileList = Get-ChildItem -Path $folderPath -Recurse | ForEach-Object { $_.FullName }

if ($fileList -is [string]) {
    $fileList = @($fileList)
}

if (-not $cmakeFileInFolder -and (Test-Path $cmakeFilePath)) {
    $fileList += $cmakeFilePath
}

Write-Output $fileList
Compress-Archive -Path $fileList -DestinationPath $zipPath
