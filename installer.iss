; Inno Setup script for REW SPL Meter Bridge
; Build with: iscc /DMyAppVersion=X.Y.Z installer.iss

#ifndef MyAppVersion
  #define MyAppVersion "0.0.0"
#endif

#define MyAppName "REW SPL Bridge"
#define MyAppPublisher "REW SPL Bridge"
#define MyAppExeName "REW SPL Bridge.exe"

[Setup]
AppId={{A1B2C3D4-E5F6-7890-ABCD-EF1234567890}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
DefaultDirName={autopf}\{#MyAppName}
DefaultGroupName={#MyAppName}
DisableProgramGroupPage=yes
OutputDir=installer_output
OutputBaseFilename=REW-Bridge-Setup-{#MyAppVersion}
Compression=lzma
SolidCompression=yes
WizardStyle=modern
PrivilegesRequired=admin
SetupIconFile=app_icon.ico
UninstallDisplayIcon={app}\{#MyAppExeName}

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: checkedonce
Name: "autostart"; Description: "Start automatically when Windows starts"; GroupDescription: "Startup:"
Name: "firewall"; Description: "Add Windows Firewall rule (recommended)"; GroupDescription: "Network:"; Flags: checkedonce
Name: "rewgui"; Description: "Show REW GUI when running (default: headless)"; GroupDescription: "REW:"

[Files]
Source: "dist\REW SPL Bridge\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{group}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"
Name: "{group}\Uninstall {#MyAppName}"; Filename: "{uninstallexe}"
Name: "{autodesktop}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; Tasks: desktopicon

[Registry]
Root: HKCU; Subkey: "Software\Microsoft\Windows\CurrentVersion\Run"; ValueType: string; ValueName: "{#MyAppName}"; ValueData: """{app}\{#MyAppExeName}"""; Flags: uninsdeletevalue; Tasks: autostart

[Run]
Filename: "{app}\{#MyAppExeName}"; Description: "Launch {#MyAppName}"; Flags: nowait postinstall skipifsilent runasoriginaluser

[Code]
const
  FirewallRuleName = 'REW SPL Bridge';

procedure AddFirewallRule;
var
  ResultCode: Integer;
begin
  // Delete existing rule first (idempotent)
  Exec('netsh.exe', 'advfirewall firewall delete rule name="' + FirewallRuleName + '"',
       '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
  // Add new rule for the exe
  Exec('netsh.exe',
       'advfirewall firewall add rule name="' + FirewallRuleName + '" dir=in action=allow protocol=tcp program="' + ExpandConstant('{app}\{#MyAppExeName}') + '" enable=yes',
       '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
end;

procedure RemoveFirewallRule;
var
  ResultCode: Integer;
begin
  Exec('netsh.exe', 'advfirewall firewall delete rule name="' + FirewallRuleName + '"',
       '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
end;

procedure SetRewGui;
var
  ConfigPath: String;
  RawContent: AnsiString;
  Content: String;
begin
  ConfigPath := ExpandConstant('{localappdata}\REW SPL Bridge\config.json');
  if FileExists(ConfigPath) then
  begin
    if LoadStringFromFile(ConfigPath, RawContent) then
    begin
      Content := RawContent;
      StringChangeEx(Content, '"rew_gui": false', '"rew_gui": true', True);
      SaveStringToFile(ConfigPath, Content, False);
    end;
  end;
end;

procedure CreateDefaultConfig;
var
  ConfigDir: String;
  ConfigPath: String;
begin
  ConfigDir := ExpandConstant('{localappdata}\REW SPL Bridge');
  ForceDirectories(ConfigDir);
  ConfigPath := ConfigDir + '\config.json';
  if not FileExists(ConfigPath) then
  begin
    SaveStringToFile(ConfigPath,
      '{' + #13#10 +
      '    "rew_path": null,' + #13#10 +
      '    "bridge_port": 8080,' + #13#10 +
      '    "rew_api_port": 4735,' + #13#10 +
      '    "log_level": "INFO",' + #13#10 +
      '    "rew_gui": false' + #13#10 +
      '}' + #13#10,
      False);
  end;
end;

procedure CurStepChanged(CurStep: TSetupStep);
begin
  if CurStep = ssPostInstall then
  begin
    CreateDefaultConfig;
    if IsTaskSelected('rewgui') then
      SetRewGui;
    if IsTaskSelected('firewall') then
      AddFirewallRule;
  end;
end;

procedure CurUninstallStepChanged(CurUninstallStep: TUninstallStep);
begin
  if CurUninstallStep = usPostUninstall then
  begin
    RemoveFirewallRule;
    // config.json and logs are left behind intentionally (user data)
  end;
end;
