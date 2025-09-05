param (
    [string]$Mode,              # "split" or "join"
    [string]$SourceFile,        # Full path to the source file (for split) or chunk folder (for join)
    [string]$OutputFolder,      # Folder to store chunks or final output
    [int]$ChunkSizeMB = 10      # Max chunk size in MB (used only in split mode)
)

function Split-File {
    $chunkSize = $ChunkSizeMB * 1MB
    $buffer = New-Object byte[] $chunkSize


    if (-not (Test-Path $OutputFolder)) {
        New-Item -ItemType Directory -Path $OutputFolder | Out-Null
    }

    $reader = [System.IO.File]::OpenRead($SourceFile)
    $baseName = [System.IO.Path]::GetFileName($SourceFile)
    $index = 0

    while ($reader.Position -lt $reader.Length) {
        $bytesRead = $reader.Read($buffer, 0, $chunkSize)
        $chunkFile = Join-Path $OutputFolder "$baseName.part$index"
        [System.IO.File]::WriteAllBytes($chunkFile, $buffer[0..($bytesRead - 1)])
        $index++
    }

    $reader.Close()
    Write-Host "Split complete: $index chunks created."
}

function Join-Files {
    $chunkFiles = Get-ChildItem -Path $SourceFile -Filter "*.part*" | Sort-Object Name
    $baseName = ($chunkFiles[0].Name -replace "\.part\d+$", "")
    $outputFile = Join-Path $OutputFolder $baseName
    $writer = [System.IO.File]::Create($outputFile)

    foreach ($chunk in $chunkFiles) {
        $bytes = [System.IO.File]::ReadAllBytes($chunk.FullName)
        $writer.Write($bytes, 0, $bytes.Length)
    }

    $writer.Close()
    Write-Host "Join complete: $outputFile created."
}

if ($Mode -eq "split") {
    Split-File
} elseif ($Mode -eq "join") {
    Join-Files
} else {
    Write-Host "Invalid mode. Use 'split' or 'join'."
}