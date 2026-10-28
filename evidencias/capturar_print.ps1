# Abre uma janela NOVA do Windows Terminal, roda comandos bash nela e salva um print SÓ dessa janela
# (PrintWindow: captura o conteúdo da janela mesmo que outra esteja por cima). Ao final o bash
# termina sozinho e a janela fecha; nenhuma outra janela do terminal é tocada.
#   powershell -ExecutionPolicy Bypass -File evidencias\capturar_print.ps1 -Saida evidencias\prints\x.png -Comandos "./scripts/status.sh" -Espera 8
param(
  [Parameter(Mandatory)] [string]$Saida,
  [Parameter(Mandatory)] [string]$Comandos,
  [int]$Espera = 8,
  [int]$Colunas = 165,
  [int]$Linhas = 46
)
Add-Type -AssemblyName System.Drawing
Add-Type @"
using System; using System.Text; using System.Collections.Generic; using System.Runtime.InteropServices;
public static class Janela {
  public delegate bool Visitar(IntPtr h, IntPtr l);
  [DllImport("user32.dll")] static extern bool EnumWindows(Visitar f, IntPtr l);
  [DllImport("user32.dll")] static extern int GetClassName(IntPtr h, StringBuilder s, int n);
  [DllImport("user32.dll")] public static extern bool GetWindowRect(IntPtr h, out RECT r);
  [DllImport("user32.dll")] public static extern bool PrintWindow(IntPtr h, IntPtr hdc, uint f);
  [StructLayout(LayoutKind.Sequential)] public struct RECT { public int L, T, R, B; }
  public static List<IntPtr> Terminais() {
    var r = new List<IntPtr>();
    EnumWindows((h, l) => { var s = new StringBuilder(64); GetClassName(h, s, 64);
      if (s.ToString() == "CASCADIA_HOSTING_WINDOW_CLASS") r.Add(h); return true; }, IntPtr.Zero);
    return r;
  }
}
"@
$raiz = Split-Path -Parent $PSScriptRoot
$bash = "C:\Program Files\Git\bin\bash.exe"
$tmp = Join-Path ([IO.Path]::GetTempPath()) ("print_" + [guid]::NewGuid().ToString("N"))
$fim = "$tmp.fim"
# prompt "$ comando" antes de cada linha, para o print mostrar o comando e a saída real
$script = "cd '$($raiz -replace '\\','/')'; export MSYS_NO_PATHCONV=1; clear`n" +
  "while IFS= read -r l; do printf '\033[1;32m`$\033[0m %s\n' `"`$l`"; eval `"`$l`"; done <<'FIM'`n$Comandos`nFIM`n" +
  "while [ ! -f '$($fim -replace '\\','/')' ]; do sleep 0.3; done`n"
[IO.File]::WriteAllText("$tmp.sh", $script.Replace("`r", ""), (New-Object Text.UTF8Encoding $false))

$antes = [Janela]::Terminais()
Start-Process wt.exe -ArgumentList "-w new --size $Colunas,$Linhas --title `"evidencia`" `"$bash`" `"$($tmp -replace '\\','/').sh`""
$h = [IntPtr]::Zero
for ($i = 0; $i -lt 50 -and $h -eq [IntPtr]::Zero; $i++) {
  Start-Sleep -Milliseconds 200
  $h = [Janela]::Terminais() | Where-Object { $antes -notcontains $_ } | Select-Object -First 1
  if (-not $h) { $h = [IntPtr]::Zero }
}
if ($h -eq [IntPtr]::Zero) { throw "janela nova do terminal não apareceu" }
Start-Sleep -Seconds $Espera
$r = New-Object Janela+RECT
[Janela]::GetWindowRect($h, [ref]$r) | Out-Null
$bmp = New-Object Drawing.Bitmap ($r.R - $r.L), ($r.B - $r.T)
$g = [Drawing.Graphics]::FromImage($bmp); $hdc = $g.GetHdc()
[Janela]::PrintWindow($h, $hdc, 2) | Out-Null
$g.ReleaseHdc($hdc); $g.Dispose()
New-Item -ItemType Directory -Force (Split-Path -Parent $Saida) | Out-Null
$bmp.Save($Saida, [Drawing.Imaging.ImageFormat]::Png); $bmp.Dispose()
New-Item -ItemType File $fim | Out-Null   # o bash vê o arquivo, termina e a janela fecha
Start-Sleep -Seconds 1
Remove-Item "$tmp.sh", $fim -ErrorAction SilentlyContinue
"print salvo em $Saida"
