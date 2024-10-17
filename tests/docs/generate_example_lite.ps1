$env:PYTHONIOENCODING = "UTF-8"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

$CommandsFile = "D:\Sync\repos\els\tests\docs\running_example_lite.ps1"
$exampleMarkdownFile = "D:\Sync\repos\els\tests\docs\running_example_lite.md"
$controlsDir = "D:\Sync\repos\els\tests\docs\controls_lite\"
$CurrentMarkdownFile = ""

# Ensure the example markdown file is empty at the start
"# Examples`n" | Set-Content $exampleMarkdownFile

rm ($controlsDir + '*.*')

function CloseAndWriteMarkdown {
    param($markdownContent, $MarkdownFile)
    if ($markdownContent.Count -gt 1) {
        if ($markdownContent[-1] -eq "") {
            $markdownContent = $markdownContent[0..($markdownContent.Count - 2)]
        }
        if ($markdownContent[0] -eq "") {
            $markdownContent = $markdownContent[1..($markdownContent.Count - 1)]
        }
        if ($markdownContent[0] -eq "") {
            $markdownContent = $markdownContent[1..($markdownContent.Count - 1)]
        }
        $markdownContent += "``````"
        $fileName = Split-Path -Path $MarkdownFile -Leaf
        $modifiedTime = (Get-Item -Path $MarkdownFile).LastWriteTime
        # $modifiedTime = (Get-Item -Path $MarkdownFile).LastWriteTime.ToString("yyyy-MM-dd HH:mm")
        $exampleContent = @"
## $fileName
$modifiedTime

``````{.include }
$fileName
``````

"@
        $markdownContent | Add-Content $MarkdownFile
        $exampleContent | Add-Content $exampleMarkdownFile
    }
}

$markdownContent = @()

Get-Content $CommandsFile | ForEach-Object {
    $line = $_.Trim()

    if ($line.StartsWith("#") -and !$line.StartsWith("##") -and !$line.EndsWith("#rem")) {
        if ($line -match "^#([^\s]+)\s+(.*)$") {
            CloseAndWriteMarkdown -markdownContent $markdownContent -MarkdownFile $CurrentMarkdownFile
            $CurrentMarkdownFile = $controlsDir + $matches[1] + '.md'
            "" | Set-Content $CurrentMarkdownFile
            $markdownContent = @("``````{.console #id" + $matches[1] + " caption=""" + $matches[2] + """}")
        }
    }
    elseif (!$line.StartsWith("#")) {
        $Output = Invoke-Expression $line 2>&1
        # Add the command and its output to the markdown content array
        
        if ($markdownContent.Count -eq 0) {
            $markdownContent += "``````console"
        }

        # if line does not end with #rem
        if (!$line.EndsWith("#rem")) {
            $markdownContent += "$ $line"
            $markdownContent += $Output
        }
        
    }
}

# Ensure to close the last markdown block if any
CloseAndWriteMarkdown -markdownContent $markdownContent -MarkdownFile $CurrentMarkdownFile

# revert back to cwd
# Set-Location '~\els-demo\'
# Set-Location 'D:\Sync\repos\els-Dis\_md'