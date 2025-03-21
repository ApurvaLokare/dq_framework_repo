<# Attention Windows User :  Try to execute this Powershell Script in Powershell environment and if 
you receive an error that " build.ps1 can not be loaded because running scripts is disabled on this 
system."  Then follow the instruction given in this link https://tecadmin.net/powershell-running-scripts-is-disabled-system/

Commane to Run:
powershell -ExecutionPolicy ByPass -File build_project_reports.ps1  -target "build" -layer "project-reports-common-wheel-ver-1" -zip "project-reports-common-layer-ver-1"
 #>

[cmdletbinding(SupportsShouldProcess = $True)]
param(
    [Parameter(Position = 1)]
    [String]
    $target = "build",
    [String]
    $layer = "dq_framework-common-wheel-ver-1",
    [String]
    $zip = "dq_framework-common-layer-ver-1"
)

# Get Current Location
$currentPath = "$PSScriptRoot"
$pythonArtifacts = "$currentPath\python"
$buildArtifacts = "$currentPath\build"
$setupFile = "$currentPath\build.py"
$whlFilePath = "$currentPath\dist"
$eggFilePath = "$currentPath\dq_framework.egg-info"


function check_folder($folder_name) {

    <#
        .SYNOPSIS
        Check existence of a folder , and if it does not exist then creates one
    #>
    if (Test-Path $folder_name) {
        Write-Host "$folder_name Exists, and it is okay"
    }
    else
    {
        #PowerShell Create directory if not exists
        Write-Host "$folder_name Does not exist"
        New-Item $folder_name -ItemType Directory
        Write-Host "$folder_name Created successfully"
    }
}

function build_lambda_layer {
    <#
        .SYNOPSIS
        Creates a zip package with the specific folder structure required by AWS Lambda
    #>
    Write-Host "Started Creating Layer..."
   
    check_folder($pythonArtifacts)
    check_folder($whlFilePath)
    $package_whl_name = $layer + ".whl"
    $package_zip_name = $zip + ".zip"
    #$package_path = "$pythonArtifacts/$package_name"

    # 1. Ensure we start with a clean python folder
    Remove-Item -Force -Recurse $pythonArtifacts -ErrorAction SilentlyContinue
    
    # 2. Prepare whl file under dist/ directory.
    Write-Host "Building whl File..."
    python $setupFile bdist_wheel --dist-dir $whlFilePath | Out-Null

    # 3. Get File Name of Created whl File.
    $whlFileName = Get-ChildItem -Path $whlFilePath\* -Include *.whl | Select Name

    # 4. Install packages from whl file.
    Write-Host "Building zip File... $whlFileName"
    pip  --trusted-host pypi.org --trusted-host files.pythonhosted.org install $whlFilePath\$($whlFileName."Name") --upgrade -t $pythonArtifacts | Out-Null
    
    # 5. copy the files to the artifacts/pyton folder and zip them
    # New-Item likes to list the path. let's silent it with | Out-Null
    New-Item -ItemType Directory -Force $pythonArtifacts | Out-Null

    if ($IsWindows -or ($ENV:OS -eq "Windows_NT")) {
        Write-Host "Windows Platform - Granting Permission to Python Artifacts"
        icacls $pythonArtifacts /grant Everyone:F /t
        icacls $whlFilePath /grant Everyone:F /t
        icacls $eggFilePath /grant Everyone:F /t
    } else {
        Write-Host "Non Windows Platform - Granting Permission with chmod -R 755"
        chmod -R 755 $pythonArtifacts
        chmod -R 755 $whlFilePath
        chmod -R 755 $eggFilePath
    }
 
    if ($IsWindows -or ($ENV:OS -eq "Windows_NT")) {
        # Lambda Layer upload fails when zipped like above..
        Write-Host "Now creating a zip file  ( Windows): $package_zip_name"
        Compress-Archive -Force -Path $pythonArtifacts -Destination "$PSScriptRoot/$package_zip_name"

        # Move whl file to current directory
        Move-Item -Force -Path $whlFilePath\$($whlFileName."Name") $currentPath\$package_whl_name

    } else {
      
        # And now gets zipped in such a way that when unzipped it begins with python level folder only
        Write-Host "Now creating a zip file  ( Non-Windows): $package_name"
        zip -r -X $package_zip_name python
    }

    # 3. Remove the pyton folders and the files just to leave everything tidy
    Write-Host "Now Cleaning up fles in : $pythonArtifacts "
    Remove-Item -Force -Recurse $pythonArtifacts

    Write-Host "Now Cleaning up fles in : $buildArtifacts "
    Remove-Item -Force -Recurse $buildArtifacts

    Write-Host "Now Cleaning up fles in : $whlFilePath"
    Remove-Item -Force -Recurse $whlFilePath 

    Write-Host "Now Cleaning up fles in : $eggFilePath "
    Remove-Item -Force -Recurse $eggFilePath 

    Write-Host " Cleanup is Completed."
    Write-Output "Created $package_zip_name."
    Write-Output "Created $package_whl_name."

}

if ($target -like "build") { 
    build_lambda_layer; 
    }