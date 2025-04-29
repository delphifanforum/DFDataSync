unit DFDataSync;

{**
 * DFDataSync - Data Synchronization and Replication Framework for Delphi
 *
 * Created by: DelphiFan
 * Date: April 20, 2025
 *
 * This unit provides components and classes for robust data replication
 * between local and remote data sources with conflict resolution,
 * change tracking, and efficient differential synchronization.
 *}

interface

uses
  System.SysUtils, System.Classes, System.Generics.Collections,
  System.Threading, System.SyncObjs, System.DateUtils, System.JSON,
  Data.DB, FireDAC.Comp.Client, FireDAC.Comp.DataSet,
  FireDAC.Stan.Intf, FireDAC.Stan.Option;

type
  /// <summary>
  /// Enum for synchronization directions
  /// </summary>
  TDFSyncDirection = (sdUpload, sdDownload, sdBidirectional);

  /// <summary>
  /// Enum for conflict resolution strategies
  /// </summary>
  TDFConflictResolution = (crSourceWins, crTargetWins, crNewerWins, crManual);
  
  /// <summary>
  /// Record to track change information
  /// </summary>
  TDFChangeInfo = record
    EntityName: string;
    PrimaryKey: Variant;
    ChangeType: string; // 'Insert', 'Update', 'Delete'
    ChangeTime: TDateTime;
    UserID: string;
    DeviceID: string;
    VersionNumber: Integer;
    ChangedFields: TArray<string>;
  end;

  /// <summary>
  /// Interface for change tracking
  /// </summary>
  IDFChangeTracker = interface
    ['{F3A72BC1-9E45-4D2F-A31D-B8E7D3C56A12}']
    procedure TrackInsert(const EntityName: string; const PrimaryKey: Variant);
    procedure TrackUpdate(const EntityName: string; const PrimaryKey: Variant; const ChangedFields: TArray<string>);
    procedure TrackDelete(const EntityName: string; const PrimaryKey: Variant);
    function GetChanges(const SinceTimestamp: TDateTime; const EntityFilter: TArray<string> = nil): TArray<TDFChangeInfo>;
    function GetLastSyncTimestamp: TDateTime;
    procedure SetLastSyncTimestamp(const Value: TDateTime);
    property LastSyncTimestamp: TDateTime read GetLastSyncTimestamp write SetLastSyncTimestamp;
  end;

  /// <summary>
  /// Change tracker implementation that uses a database table
  /// </summary>
  TDFDBChangeTracker = class(TInterfacedObject, IDFChangeTracker)
  private
    FConnection: TFDConnection;
    FChangesTable: string;
    FSyncStateTable: string;
    FDeviceID: string;
    FUserID: string;
  public
    constructor Create(AConnection: TFDConnection; 
                        const AChangesTable: string = 'DFChangeLog';
                        const ASyncStateTable: string = 'DFSyncState';
                        const ADeviceID: string = '';
                        const AUserID: string = '');
    
    // IDFChangeTracker implementation
    procedure TrackInsert(const EntityName: string; const PrimaryKey: Variant);
    procedure TrackUpdate(const EntityName: string; const PrimaryKey: Variant; const ChangedFields: TArray<string>);
    procedure TrackDelete(const EntityName: string; const PrimaryKey: Variant);
    function GetChanges(const SinceTimestamp: TDateTime; const EntityFilter: TArray<string> = nil): TArray<TDFChangeInfo>;
    function GetLastSyncTimestamp: TDateTime;
    procedure SetLastSyncTimestamp(const Value: TDateTime);
    
    // Helper methods
    procedure InitializeTables;
    procedure CleanupOldChanges(const DaysToKeep: Integer = 30);
  end;

  /// <summary>
  /// Event for conflict notification
  /// </summary>
  TDFConflictEvent = procedure(Sender: TObject; 
                              const EntityName: string;
                              const PrimaryKey: Variant; 
                              var Resolution: TDFConflictResolution) of object;

  /// <summary>
  /// Event for progress updates
  /// </summary>
  TDFProgressEvent = procedure(Sender: TObject; 
                              const CurrentItem, TotalItems: Integer;
                              const EntityName: string;
                              const OperationType: string) of object;

  /// <summary>
  /// Interface for data synchronization endpoints
  /// </summary>
  IDFSyncEndpoint = interface
    ['{D9E45A21-B7CF-4623-88D3-A5C201D83F79}']
    function GetEntityNames: TArray<string>;
    function GetEntityData(const EntityName: string; const SinceTimestamp: TDateTime = 0): TFDMemTable;
    function GetChangeTracker: IDFChangeTracker;
    procedure ApplyChanges(const EntityName: string; Changes: TFDMemTable; ChangeInfo: TArray<TDFChangeInfo>);
    function ValidateEntity(const EntityName: string): Boolean;
  end;

  /// <summary>
  /// Local database synchronization endpoint
  /// </summary>
  TDFLocalDatabaseEndpoint = class(TInterfacedObject, IDFSyncEndpoint)
  private
    FConnection: TFDConnection;
    FChangeTracker: IDFChangeTracker;
    FSyncEntities: TDictionary<string, string>; // EntityName -> TableName/SQL
  public
    constructor Create(AConnection: TFDConnection; AChangeTracker: IDFChangeTracker = nil);
    destructor Destroy; override;
    
    // Add entity for synchronization (table name or SQL query)
    procedure AddEntity(const EntityName, TableNameOrSQL: string);
    procedure RemoveEntity(const EntityName: string);
    
    // IDFSyncEndpoint implementation
    function GetEntityNames: TArray<string>;
    function GetEntityData(const EntityName: string; const SinceTimestamp: TDateTime = 0): TFDMemTable;
    function GetChangeTracker: IDFChangeTracker;
    procedure ApplyChanges(const EntityName: string; Changes: TFDMemTable; ChangeInfo: TArray<TDFChangeInfo>);
    function ValidateEntity(const EntityName: string): Boolean;
  end;

  /// <summary>
  /// Main synchronization controller
  /// </summary>
  TDFSyncController = class(TComponent)
  private
    FSourceEndpoint: IDFSyncEndpoint;
    FTargetEndpoint: IDFSyncEndpoint;
    FSyncDirection: TDFSyncDirection;
    FConflictResolution: TDFConflictResolution;
    FOnConflict: TDFConflictEvent;
    FOnProgress: TDFProgressEvent;
    FOnSyncComplete: TNotifyEvent;
    FOnSyncError: TProc<string>;
    FSyncTask: ITask;
    FSyncEntities: TArray<string>;
    FIsSyncing: Boolean;
    FSyncInterval: Integer; // in seconds, 0 = manual only
    FSyncTimer: TTimer;
    FLastSyncTime: TDateTime;
    
    procedure DoSyncTimer(Sender: TObject);
    procedure SetSyncInterval(const Value: Integer);
    function HandleConflict(const EntityName: string; const PrimaryKey: Variant): TDFConflictResolution;
    procedure ReportProgress(const CurrentItem, TotalItems: Integer; const EntityName, OperationType: string);
  protected
    procedure SynchronizeEntity(const EntityName: string);
  public
    constructor Create(AOwner: TComponent); override;
    destructor Destroy; override;
    
    // Configuration methods
    procedure SetEndpoints(ASourceEndpoint, ATargetEndpoint: IDFSyncEndpoint);
    procedure SetSyncEntities(const EntityNames: TArray<string>);
    
    // Synchronization operations
    procedure StartSync(const AEntities: TArray<string> = nil);
    procedure CancelSync;
    function IsSyncing: Boolean;
    
    // Properties
    property SourceEndpoint: IDFSyncEndpoint read FSourceEndpoint write FSourceEndpoint;
    property TargetEndpoint: IDFSyncEndpoint read FTargetEndpoint write FTargetEndpoint;
    property SyncDirection: TDFSyncDirection read FSyncDirection write FSyncDirection;
    property ConflictResolution: TDFConflictResolution read FConflictResolution write FConflictResolution;
    property SyncInterval: Integer read FSyncInterval write SetSyncInterval;
    property LastSyncTime: TDateTime read FLastSyncTime;
    
    // Events
    property OnConflict: TDFConflictEvent read FOnConflict write FOnConflict;
    property OnProgress: TDFProgressEvent read FOnProgress write FOnProgress;
    property OnSyncComplete: TNotifyEvent read FOnSyncComplete write FOnSyncComplete;
    property OnSyncError: TProc<string> read FOnSyncError write FOnSyncError;
  end;

  /// <summary>
  /// Helper class for differential data comparison
  /// </summary>
  TDFDiffEngine = class
  private
    class function ComputeRowHash(Dataset: TDataSet): string;
  public
    class function CompareDatasets(Source, Target: TDataSet; const KeyFields: string): TFDMemTable;
    class function ExtractChangedRecords(Source, Target: TDataSet; const KeyFields: string): TFDMemTable;
  end;

implementation

uses
  System.Variants, System.Hash, System.IOUtils;

{ TDFDBChangeTracker }

constructor TDFDBChangeTracker.Create(AConnection: TFDConnection; const AChangesTable,
  ASyncStateTable, ADeviceID, AUserID: string);
begin
  inherited Create;
  FConnection := AConnection;
  FChangesTable := AChangesTable;
  FSyncStateTable := ASyncStateTable;
  
  // If device ID not provided, generate from machine info
  if ADeviceID = '' then
    FDeviceID := THashMD5.GetHashString(TOSVersion.ToString + GetComputerName)
  else
    FDeviceID := ADeviceID;
    
  FUserID := AUserID;
  
  InitializeTables;
end;

procedure TDFDBChangeTracker.InitializeTables;
begin
  // Create change log table if it doesn't exist
  FConnection.ExecSQL(
    'CREATE TABLE IF NOT EXISTS ' + FChangesTable + ' (' +
    '  ID INTEGER PRIMARY KEY AUTOINCREMENT,' +
    '  EntityName VARCHAR(100) NOT NULL,' +
    '  PrimaryKey VARCHAR(255) NOT NULL,' +
    '  ChangeType VARCHAR(20) NOT NULL,' + // Insert, Update, Delete
    '  ChangeTime DATETIME NOT NULL,' +
    '  UserID VARCHAR(50),' +
    '  DeviceID VARCHAR(50) NOT NULL,' +
    '  VersionNumber INTEGER NOT NULL,' +
    '  ChangedFields TEXT' +
    ')');

  // Create index on EntityName and PrimaryKey
  FConnection.ExecSQL(
    'CREATE INDEX IF NOT EXISTS IDX_' + FChangesTable + '_Entity_PK ON ' + 
    FChangesTable + '(EntityName, PrimaryKey)');

  // Create sync state table if it doesn't exist
  FConnection.ExecSQL(
    'CREATE TABLE IF NOT EXISTS ' + FSyncStateTable + ' (' +
    '  ID INTEGER PRIMARY KEY AUTOINCREMENT,' +
    '  DeviceID VARCHAR(50) NOT NULL,' +
    '  LastSyncTime DATETIME,' +
    '  UNIQUE(DeviceID)' +
    ')');
    
  // Make sure this device has a sync state entry
  FConnection.ExecSQL(
    'INSERT OR IGNORE INTO ' + FSyncStateTable + '(DeviceID, LastSyncTime) ' +
    'VALUES(:DeviceID, :LastSyncTime)',
    [FDeviceID, Now]);
end;

procedure TDFDBChangeTracker.TrackInsert(const EntityName: string; const PrimaryKey: Variant);
begin
  FConnection.ExecSQL(
    'INSERT INTO ' + FChangesTable + 
    '(EntityName, PrimaryKey, ChangeType, ChangeTime, UserID, DeviceID, VersionNumber, ChangedFields) ' +
    'VALUES(:EntityName, :PrimaryKey, :ChangeType, :ChangeTime, :UserID, :DeviceID, ' +
    '(SELECT COALESCE(MAX(VersionNumber), 0) + 1 FROM ' + FChangesTable + 
    ' WHERE EntityName = :EntityName AND PrimaryKey = :PrimaryKey), :ChangedFields)',
    [EntityName, VarToStr(PrimaryKey), 'Insert', Now, FUserID, FDeviceID, '']);
end;

procedure TDFDBChangeTracker.TrackUpdate(const EntityName: string; const PrimaryKey: Variant; 
  const ChangedFields: TArray<string>);
var
  FieldsStr: string;
begin
  FieldsStr := string.Join(',', ChangedFields);
  
  FConnection.ExecSQL(
    'INSERT INTO ' + FChangesTable + 
    '(EntityName, PrimaryKey, ChangeType, ChangeTime, UserID, DeviceID, VersionNumber, ChangedFields) ' +
    'VALUES(:EntityName, :PrimaryKey, :ChangeType, :ChangeTime, :UserID, :DeviceID, ' +
    '(SELECT COALESCE(MAX(VersionNumber), 0) + 1 FROM ' + FChangesTable + 
    ' WHERE EntityName = :EntityName AND PrimaryKey = :PrimaryKey), :ChangedFields)',
    [EntityName, VarToStr(PrimaryKey), 'Update', Now, FUserID, FDeviceID, FieldsStr]);
end;

procedure TDFDBChangeTracker.TrackDelete(const EntityName: string; const PrimaryKey: Variant);
begin
  FConnection.ExecSQL(
    'INSERT INTO ' + FChangesTable + 
    '(EntityName, PrimaryKey, ChangeType, ChangeTime, UserID, DeviceID, VersionNumber, ChangedFields) ' +
    'VALUES(:EntityName, :PrimaryKey, :ChangeType, :ChangeTime, :UserID, :DeviceID, ' +
    '(SELECT COALESCE(MAX(VersionNumber), 0) + 1 FROM ' + FChangesTable + 
    ' WHERE EntityName = :EntityName AND PrimaryKey = :PrimaryKey), :ChangedFields)',
    [EntityName, VarToStr(PrimaryKey), 'Delete', Now, FUserID, FDeviceID, '']);
end;

function TDFDBChangeTracker.GetChanges(const SinceTimestamp: TDateTime;
  const EntityFilter: TArray<string>): TArray<TDFChangeInfo>;
var
  Query: TFDQuery;
  WhereClause: string;
  I: Integer;
  EntityList: string;
  Info: TDFChangeInfo;
begin
  Query := TFDQuery.Create(nil);
  try
    Query.Connection := FConnection;
    
    WhereClause := 'ChangeTime > :SinceTime';
    
    // Add entity filter if provided
    if Length(EntityFilter) > 0 then
    begin
      EntityList := '';
      for I := 0 to High(EntityFilter) do
      begin
        if EntityList <> '' then
          EntityList := EntityList + ',';
        EntityList := EntityList + QuotedStr(EntityFilter[I]);
      end;
      WhereClause := WhereClause + ' AND EntityName IN (' + EntityList + ')';
    end;
    
    Query.SQL.Text := 
      'SELECT EntityName, PrimaryKey, ChangeType, ChangeTime, UserID, DeviceID, VersionNumber, ChangedFields ' +
      'FROM ' + FChangesTable + ' ' +
      'WHERE ' + WhereClause + ' ' +
      'ORDER BY ChangeTime, EntityName, PrimaryKey';
      
    Query.ParamByName('SinceTime').AsDateTime := SinceTimestamp;
    Query.Open;
    
    SetLength(Result, Query.RecordCount);
    I := 0;
    
    while not Query.Eof do
    begin
      Info.EntityName := Query.FieldByName('EntityName').AsString;
      Info.PrimaryKey := Query.FieldByName('PrimaryKey').AsVariant;
      Info.ChangeType := Query.FieldByName('ChangeType').AsString;
      Info.ChangeTime := Query.FieldByName('ChangeTime').AsDateTime;
      Info.UserID := Query.FieldByName('UserID').AsString;
      Info.DeviceID := Query.FieldByName('DeviceID').AsString;
      Info.VersionNumber := Query.FieldByName('VersionNumber').AsInteger;
      
      // Parse changed fields string into array
      if Query.FieldByName('ChangedFields').AsString <> '' then
        Info.ChangedFields := Query.FieldByName('ChangedFields').AsString.Split([','])
      else
        SetLength(Info.ChangedFields, 0);
        
      Result[I] := Info;
      Inc(I);
      
      Query.Next;
    end;
  finally
    Query.Free;
  end;
end;

function TDFDBChangeTracker.GetLastSyncTimestamp: TDateTime;
var
  Query: TFDQuery;
begin
  Result := 0;
  
  Query := TFDQuery.Create(nil);
  try
    Query.Connection := FConnection;
    Query.SQL.Text := 
      'SELECT LastSyncTime FROM ' + FSyncStateTable + ' WHERE DeviceID = :DeviceID';
    Query.ParamByName('DeviceID').AsString := FDeviceID;
    Query.Open;
    
    if not Query.Eof then
      Result := Query.FieldByName('LastSyncTime').AsDateTime;
  finally
    Query.Free;
  end;
end;

procedure TDFDBChangeTracker.SetLastSyncTimestamp(const Value: TDateTime);
begin
  FConnection.ExecSQL(
    'UPDATE ' + FSyncStateTable + ' SET LastSyncTime = :LastSyncTime WHERE DeviceID = :DeviceID',
    [Value, FDeviceID]);
end;

procedure TDFDBChangeTracker.CleanupOldChanges(const DaysToKeep: Integer);
var
  CutoffDate: TDateTime;
begin
  CutoffDate := IncDay(Now, -DaysToKeep);
  
  FConnection.ExecSQL(
    'DELETE FROM ' + FChangesTable + ' WHERE ChangeTime < :CutoffDate',
    [CutoffDate]);
end;

{ TDFLocalDatabaseEndpoint }

constructor TDFLocalDatabaseEndpoint.Create(AConnection: TFDConnection; AChangeTracker: IDFChangeTracker);
begin
  inherited Create;
  FConnection := AConnection;
  FSyncEntities := TDictionary<string, string>.Create;
  
  // Create change tracker if not provided
  if AChangeTracker = nil then
    FChangeTracker := TDFDBChangeTracker.Create(FConnection)
  else
    FChangeTracker := AChangeTracker;
end;

destructor TDFLocalDatabaseEndpoint.Destroy;
begin
  FSyncEntities.Free;
  inherited;
end;

procedure TDFLocalDatabaseEndpoint.AddEntity(const EntityName, TableNameOrSQL: string);
begin
  FSyncEntities.AddOrSetValue(EntityName, TableNameOrSQL);
end;

procedure TDFLocalDatabaseEndpoint.RemoveEntity(const EntityName: string);
begin
  FSyncEntities.Remove(EntityName);
end;

function TDFLocalDatabaseEndpoint.GetEntityNames: TArray<string>;
var
  List: TList<string>;
  Key: string;
begin
  List := TList<string>.Create;
  try
    for Key in FSyncEntities.Keys do
      List.Add(Key);
    Result := List.ToArray;
  finally
    List.Free;
  end;
end;

function TDFLocalDatabaseEndpoint.GetEntityData(const EntityName: string; 
  const SinceTimestamp: TDateTime): TFDMemTable;
var
  Query: TFDQuery;
  TableOrSQL: string;
  SQL: string;
begin
  Result := TFDMemTable.Create(nil);
  
  if not FSyncEntities.TryGetValue(EntityName, TableOrSQL) then
    Exit;
    
  Query := TFDQuery.Create(nil);
  try
    Query.Connection := FConnection;
    
    // Check if it's a SQL statement or table name
    if (Pos('SELECT', UpperCase(TableOrSQL)) > 0) then
    begin
      SQL := TableOrSQL;
      
      // Add timestamp filter if available and SQL doesn't already contain WHERE
      if (SinceTimestamp > 0) and (Pos('WHERE', UpperCase(SQL)) = 0) then
        SQL := SQL + ' WHERE LastModified > :SinceTimestamp';
    end
    else
    begin
      // It's a table name
      SQL := 'SELECT * FROM ' + TableOrSQL;
      
      // Add timestamp filter if available
      if SinceTimestamp > 0 then
        SQL := SQL + ' WHERE LastModified > :SinceTimestamp';
    end;
    
    Query.SQL.Text := SQL;
    
    if SinceTimestamp > 0 then
      Query.ParamByName('SinceTimestamp').AsDateTime := SinceTimestamp;
      
    Query.Open;
    
    // Copy data to the memory table
    Result.Data := Query.Data;
  finally
    Query.Free;
  end;
end;

function TDFLocalDatabaseEndpoint.GetChangeTracker: IDFChangeTracker;
begin
  Result := FChangeTracker;
end;

procedure TDFLocalDatabaseEndpoint.ApplyChanges(const EntityName: string; 
  Changes: TFDMemTable; ChangeInfo: TArray<TDFChangeInfo>);
var
  TableName: string;
  UpdateSQL, InsertSQL, DeleteSQL: string;
  FieldNames, FieldParams, PKField: string;
  I, J: Integer;
  FieldList: TStringList;
  Transaction: TFDTransaction;
  Command: TFDCommand;
  FieldName: string;
begin
  // Get the actual table name for this entity
  if not FSyncEntities.TryGetValue(EntityName, TableName) then
    Exit;
    
  // If it's a SQL query, extract the table name
  if Pos('SELECT', UpperCase(TableName)) > 0 then
  begin
    // Simple extraction of table name from SQL - in a real implementation this would be more robust
    TableName := Copy(TableName, Pos('FROM', UpperCase(TableName)) + 5, MaxInt);
    TableName := Trim(TableName.Split([' ', ',', ';'])[0]);
  end;
  
  // Assuming the primary key field is 'ID' - this would be configurable in a real implementation
  PKField := 'ID';
  
  // Build field lists for SQL statements
  FieldList := TStringList.Create;
  try
    for I := 0 to Changes.FieldCount - 1 do
    begin
      if SameText(Changes.Fields[I].FieldName, PKField) then
        Continue;
        
      FieldList.Add(Changes.Fields[I].FieldName);
    end;
    
    // Build UPDATE SQL
    UpdateSQL := 'UPDATE ' + TableName + ' SET ';
    for I := 0 to FieldList.Count - 1 do
    begin
      if I > 0 then
        UpdateSQL := UpdateSQL + ', ';
      UpdateSQL := UpdateSQL + FieldList[I] + ' = :' + FieldList[I];
    end;
    UpdateSQL := UpdateSQL + ' WHERE ' + PKField + ' = :' + PKField;
    
    // Build INSERT SQL
    FieldNames := '';
    FieldParams := '';
    for I := 0 to Changes.FieldCount - 1 do
    begin
      if I > 0 then
      begin
        FieldNames := FieldNames + ', ';
        FieldParams := FieldParams + ', ';
      end;
      FieldNames := FieldNames + Changes.Fields[I].FieldName;
      FieldParams := FieldParams + ':' + Changes.Fields[I].FieldName;
    end;
    InsertSQL := 'INSERT INTO ' + TableName + ' (' + FieldNames + ') VALUES (' + FieldParams + ')';
    
    // Build DELETE SQL
    DeleteSQL := 'DELETE FROM ' + TableName + ' WHERE ' + PKField + ' = :' + PKField;
    
    // Start transaction
    Transaction := TFDTransaction.Create(nil);
    try
      Transaction.Connection := FConnection;
      Transaction.StartTransaction;
      
      try
        Command := TFDCommand.Create(nil);
        try
          Command.Connection := FConnection;
          
          // Process all changes in the Changes dataset
          Changes.First;
          while not Changes.EOF do
          begin
            // Find corresponding change info
            for I := 0 to High(ChangeInfo) do
            begin
              if (ChangeInfo[I].EntityName = EntityName) and 
                 (VarToStr(ChangeInfo[I].PrimaryKey) = Changes.FieldByName(PKField).AsString) then
              begin
                case ChangeInfo[I].ChangeType of
                  'Insert':
                    begin
                      Command.CommandText := InsertSQL;
                      for J := 0 to Changes.FieldCount - 1 do
                        Command.Params.ParamByName(Changes.Fields[J].FieldName).Value := 
                          Changes.Fields[J].Value;
                      Command.Execute;
                      
                      // Track locally
                      FChangeTracker.TrackInsert(EntityName, Changes.FieldByName(PKField).Value);
                    end;
                    
                  'Update':
                    begin
                      Command.CommandText := UpdateSQL;
                      for J := 0 to Changes.FieldCount - 1 do
                        Command.Params.ParamByName(Changes.Fields[J].FieldName).Value := 
                          Changes.Fields[J].Value;
                      Command.Execute;
                      
                      // Track locally
                      if Length(ChangeInfo[I].ChangedFields) > 0 then
                        FChangeTracker.TrackUpdate(EntityName, Changes.FieldByName(PKField).Value, 
                          ChangeInfo[I].ChangedFields)
                      else
                      begin
                        // If changed fields not provided, deduce from data
                        FieldList.Clear;
                        for J := 0 to Changes.FieldCount - 1 do
                        begin
                          FieldName := Changes.Fields[J].FieldName;
                          if not SameText(FieldName, PKField) then
                            FieldList.Add(FieldName);
                        end;
                        FChangeTracker.TrackUpdate(EntityName, Changes.FieldByName(PKField).Value, 
                          FieldList.ToStringArray);
                      end;
                    end;
                    
                  'Delete':
                    begin
                      Command.CommandText := DeleteSQL;
                      Command.Params.ParamByName(PKField).Value := Changes.FieldByName(PKField).Value;
                      Command.Execute;
                      
                      // Track locally
                      FChangeTracker.TrackDelete(EntityName, Changes.FieldByName(PKField).Value);
                    end;
                end;
                
                Break;
              end;
            end;
            
            Changes.Next;
          end;
          
          Transaction.Commit;
        finally
          Command.Free;
        end;
      except
        Transaction.Rollback;
        raise;
      end;
    finally
      Transaction.Free;
    end;
  finally
    FieldList.Free;
  end;
end;

function TDFLocalDatabaseEndpoint.ValidateEntity(const EntityName: string): Boolean;
begin
  Result := FSyncEntities.ContainsKey(EntityName);
end;

{ TDFSyncController }

constructor TDFSyncController.Create(AOwner: TComponent);
begin
  inherited Create(AOwner);
  FSyncDirection := sdBidirectional;
  FConflictResolution := crNewerWins;
  FIsSyncing := False;
  FSyncInterval := 0;
  
  FSyncTimer := TTimer.Create(Self);
  FSyncTimer.Enabled := False;
  FSyncTimer.OnTimer := DoSyncTimer;
end;

destructor TDFSyncController.Destroy;
begin
  if Assigned(FSyncTask) then
    CancelSync;
    
  inherited;
end;

procedure TDFSyncController.SetEndpoints(ASourceEndpoint, ATargetEndpoint: IDFSyncEndpoint);
begin
  FSourceEndpoint := ASourceEndpoint;
  FTargetEndpoint := ATargetEndpoint;
end;

procedure TDFSyncController.SetSyncEntities(const EntityNames: TArray<string>);
begin
  FSyncEntities := EntityNames;
end;

procedure TDFSyncController.SetSyncInterval(const Value: Integer);
begin
  FSyncInterval := Value;
  
  if FSyncInterval > 0 then
  begin
    FSyncTimer.Interval := FSyncInterval * 1000;
    FSyncTimer.Enabled := True;
  end
  else
    FSyncTimer.Enabled := False;
end;

procedure TDFSyncController.DoSyncTimer(Sender: TObject);
begin
  if not FIsSyncing then
    StartSync;
end;

function TDFSyncController.HandleConflict(const EntityName: string; 
  const PrimaryKey: Variant): TDFConflictResolution;
begin
  Result := FConflictResolution;
  
  if Assigned(FOnConflict) then
    FOnConflict(Self, EntityName, PrimaryKey, Result);
end;

procedure TDFSyncController.ReportProgress(const CurrentItem, TotalItems: Integer; 
  const EntityName, OperationType: string);
begin
  if Assigned(FOnProgress) then
    FOnProgress(Self, CurrentItem, TotalItems, EntityName, OperationType);
end;

procedure TDFSyncController.StartSync(const AEntities: TArray<string>);
var
  EntitiesToSync: TArray<string>;
begin
  if FIsSyncing then
    Exit;
    
  if not Assigned(FSourceEndpoint) or not Assigned(FTargetEndpoint) then
    raise Exception.Create('Source and Target endpoints must be set before synchronization');
    
  // Use provided entities or all entities if nil
  if Length(AEntities) > 0 then
    EntitiesToSync := AEntities
  else if Length(FSyncEntities) > 0 then
    EntitiesToSync := FSyncEntities
  else
    EntitiesToSync := FSourceEndpoint.GetEntityNames;
    
  FIsSyncing := True;
  
  // Run synchronization in background task
  FSyncTask := TTask.Create(
    procedure
    var
      I, TotalEntities: Integer;
      Entity: string;
    begin
      try
        TotalEntities := Length(EntitiesToSync);
        
        // Perform synchronization for each entity
        for I := 0 to TotalEntities - 1 do
        begin
          Entity := EntitiesToSync[I];
          
          // Check if sync should be cancelled
          if TTask.CurrentTask.Status = TTaskStatus.Canceled then
            Break;
            
          // Report progress
          ReportProgress(I + 1, TotalEntities, Entity, 'Synchronizing');
          
          try
            SynchronizeEntity(Entity);
          except
            on E: Exception do
              if Assigned(FOnSyncError) then
                TThread.Synchronize(nil, procedure
                begin
                  FOnSyncError(Format('Error synchronizing %s: %s', [Entity, E.Message]));
                end);
          end;
        end;
        
        // Update last sync time
        FLastSyncTime := Now;
        
        // Notify completion
        if Assigned(FOnSyncComplete) then
          TThread.Synchronize(nil, procedure
          begin
            FOnSyncComplete(Self);
          end);
      finally
        TThread.Synchronize(nil, procedure
        begin
          FIsSyncing := False;
        end);
      end;
    end);
    
  FSyncTask.Start;
end;

procedure TDFSyncController.CancelSync;
begin
  if Assigned(FSyncTask) and FIsSyncing then
  begin
    FSyncTask.Cancel;
    // Wait for task to complete cancellation
    while FIsSyncing do
      Sleep(50);
  end;
end;

function TDFSyncController.IsSyncing: Boolean;
begin
  Result := FIsSyncing;
end;

procedure TDFSyncController.SynchronizeEntity(const EntityName: string);
var
  SourceData, TargetData, ChangesToApply: TFDMemTable;
  SourceChanges, TargetChanges: TArray<TDFChangeInfo>;
  LastSyncTime: TDateTime;
  I, J: Integer;
  PKField: string;
  PKValue: Variant;
  ConflictResolution: TDFConflictResolution;
  SourceTracker, TargetTracker: IDFChangeTracker;
begin
  // Validate entity on both endpoints
  if not FSourceEndpoint.ValidateEntity(EntityName) or 
     not FTargetEndpoint.ValidateEntity(EntityName) then
    Exit;
    
  SourceTracker := FSourceEndpoint.GetChangeTracker;
  TargetTracker := FTargetEndpoint.GetChangeTracker;
  
  // Get last sync time (use the minimum of both endpoints)
  LastSyncTime := Min(SourceTracker.LastSyncTimestamp, TargetTracker.LastSyncTimestamp);
  if LastSyncTime = 0 then
    LastSyncTime := EncodeDate(2000, 1, 1); // default for first sync
    
  // Get change logs since last sync
  SourceChanges := SourceTracker.GetChanges(LastSyncTime, [EntityName]);
  TargetChanges := TargetTracker.GetChanges(LastSyncTime, [EntityName]);
  
  // Handle direction-specific synchronization
  case FSyncDirection of
    sdUpload:
      begin
        // Source to Target only
        SourceData := FSourceEndpoint.GetEntityData(EntityName, LastSyncTime);
        try
          FTargetEndpoint.ApplyChanges(EntityName, SourceData, SourceChanges);
        finally
          SourceData.Free;
        end;
      end;
      
    sdDownload:
      begin
        // Target to Source only
        TargetData := FTargetEndpoint.GetEntityData(EntityName, LastSyncTime);
        try
          FSourceEndpoint.ApplyChanges(EntityName, TargetData, TargetChanges);
        finally
          TargetData.Free;
        end;
      end;
      
    sdBidirectional:
      begin
        // Bidirectional sync with conflict resolution
        SourceData := FSourceEndpoint.GetEntityData(EntityName);
        TargetData := FTargetEndpoint.GetEntityData(EntityName);
        
        try
          // Assuming primary key field is 'ID' - would be configurable in a real implementation
          PKField := 'ID';
          
          // First, check for conflicts and resolve
          for I := 0 to High(SourceChanges) do
          begin
            PKValue := SourceChanges[I].PrimaryKey;
            
            // Look for matching changes in target
            for J := 0 to High(TargetChanges) do
            begin
              if (VarToStr(TargetChanges[J].PrimaryKey) = VarToStr(PKValue)) and
                 (TargetChanges[J].EntityName = EntityName) then
              begin
                // Conflict detected - determine resolution
                ConflictResolution := HandleConflict(EntityName, PKValue);
                
                case ConflictResolution of
                  crSourceWins:
                    // Remove target change so source will win
                    TargetChanges[J].ChangeType := '';
                    
                  crTargetWins:
                    // Remove source change so target will win
                    SourceChanges[I].ChangeType := '';
                    
                  crNewerWins:
                    begin
                      // Compare timestamps
                      if SourceChanges[I].ChangeTime > TargetChanges[J].ChangeTime then
                        TargetChanges[J].ChangeType := ''
                      else
                        SourceChanges[I].ChangeType := '';
                    end;
                    
                  crManual:
                    begin
                      // Both are temporarily ignored, user must resolve manually
                      SourceChanges[I].ChangeType := '';
                      TargetChanges[J].ChangeType := '';
                    end;
                end;
                
                Break; // Found the matching change
              end;
            end;
          end;
          
          // Apply source changes to target
          ChangesToApply := TDFDiffEngine.ExtractChangedRecords(SourceData, TargetData, PKField);
          try
            if ChangesToApply.RecordCount > 0 then
            begin
              // Filter out changes that we decided to ignore during conflict resolution
              J := 0;
              while J <= High(SourceChanges) do
              begin
                if SourceChanges[J].ChangeType = '' then
                begin
                  // Remove this change
                  for I := J to High(SourceChanges) - 1 do
                    SourceChanges[I] := SourceChanges[I + 1];
                  SetLength(SourceChanges, Length(SourceChanges) - 1);
                end
                else
                  Inc(J);
              end;
              
              FTargetEndpoint.ApplyChanges(EntityName, ChangesToApply, SourceChanges);
            end;
          finally
            ChangesToApply.Free;
          end;
          
          // Apply target changes to source
          ChangesToApply := TDFDiffEngine.ExtractChangedRecords(TargetData, SourceData, PKField);
          try
            if ChangesToApply.RecordCount > 0 then
            begin
              // Filter out changes that we decided to ignore during conflict resolution
              J := 0;
              while J <= High(TargetChanges) do
              begin
                if TargetChanges[J].ChangeType = '' then
                begin
                  // Remove this change
                  for I := J to High(TargetChanges) - 1 do
                    TargetChanges[I] := TargetChanges[I + 1];
                  SetLength(TargetChanges, Length(TargetChanges) - 1);
                end
                else
                  Inc(J);
              end;
              
              FSourceEndpoint.ApplyChanges(EntityName, ChangesToApply, TargetChanges);
            end;
          finally
            ChangesToApply.Free;
          end;
        finally
          SourceData.Free;
          TargetData.Free;
        end;
      end;
  end;
  
  // Update last sync timestamps
  SourceTracker.LastSyncTimestamp := Now;
  TargetTracker.LastSyncTimestamp := Now;
end;

{ TDFDiffEngine }

class function TDFDiffEngine.ComputeRowHash(Dataset: TDataSet): string;
var
  I: Integer;
  FieldValue: string;
begin
  Result := '';
  
  for I := 0 to Dataset.FieldCount - 1 do
  begin
    if not Dataset.Fields[I].IsNull then
      FieldValue := Dataset.Fields[I].AsString
    else
      FieldValue := 'NULL';
      
    Result := Result + FieldValue + '|';
  end;
  
  Result := THashMD5.GetHashString(Result);
end;

class function TDFDiffEngine.CompareDatasets(Source, Target: TDataSet; 
  const KeyFields: string): TFDMemTable;
var
  SourceBookmark, TargetBookmark: TBookmark;
  SourceHashMap, TargetHashMap: TDictionary<string, string>;
  SourceKeyValue, TargetKeyValue: string;
  SourceHash, TargetHash: string;
  KeyFieldList: TStringList;
  I: Integer;
  FieldNames: string;
  FoundInTarget: Boolean;
begin
  // Create a memory table with the same structure as source
  Result := TFDMemTable.Create(nil);
  Result.Data := Source.Data;
  Result.EmptyDataSet;
  
  // Add a change type field
  Result.FieldDefs.Add('ChangeType', ftString, 10);
  Result.CreateDataSet;
  
  // Create maps to store key->hash mappings
  SourceHashMap := TDictionary<string, string>.Create;
  TargetHashMap := TDictionary<string, string>.Create;
  
  KeyFieldList := TStringList.Create;
  try
    KeyFieldList.Delimiter := ';';
    KeyFieldList.DelimitedText := KeyFields;
    
    // First pass: build hash maps for both datasets
    Source.DisableControls;
    Target.DisableControls;
    try
      SourceBookmark := Source.Bookmark;
      TargetBookmark := Target.Bookmark;
      
      try
        // Build source hash map
        Source.First;
        while not Source.Eof do
        begin
          // Build composite key value
          SourceKeyValue := '';
          for I := 0 to KeyFieldList.Count - 1 do
          begin
            if I > 0 then
              SourceKeyValue := SourceKeyValue + '|';
            SourceKeyValue := SourceKeyValue + Source.FieldByName(KeyFieldList[I]).AsString;
          end;
          
          SourceHash := ComputeRowHash(Source);
          SourceHashMap.AddOrSetValue(SourceKeyValue, SourceHash);
          
          Source.Next;
        end;
        
        // Build target hash map
        Target.First;
        while not Target.Eof do
        begin
          // Build composite key value
          TargetKeyValue := '';
          for I := 0 to KeyFieldList.Count - 1 do
          begin
            if I > 0 then
              TargetKeyValue := TargetKeyValue + '|';
            TargetKeyValue := TargetKeyValue + Target.FieldByName(KeyFieldList[I]).AsString;
          end;
          
          TargetHash := ComputeRowHash(Target);
          TargetHashMap.AddOrSetValue(TargetKeyValue, TargetHash);
          
          Target.Next;
        end;
        
        // Second pass: find differences
        Source.First;
        while not Source.Eof do
        begin
          // Build composite key value
          SourceKeyValue := '';
          for I := 0 to KeyFieldList.Count - 1 do
          begin
            if I > 0 then
              SourceKeyValue := SourceKeyValue + '|';
            SourceKeyValue := SourceKeyValue + Source.FieldByName(KeyFieldList[I]).AsString;
          end;
          
          // Check if this key exists in target
          if TargetHashMap.ContainsKey(SourceKeyValue) then
          begin
            // Key exists, compare hashes to see if content changed
            TargetHashMap.TryGetValue(SourceKeyValue, TargetHash);
            SourceHashMap.TryGetValue(SourceKeyValue, SourceHash);
            
            if SourceHash <> TargetHash then
            begin
              // Record has changed
              Result.Append;
              for I := 0 to Source.FieldCount - 1 do
                Result.Fields[I].Value := Source.Fields[I].Value;
              Result.FieldByName('ChangeType').AsString := 'Update';
              Result.Post;
            end;
          end
          else
          begin
            // Record exists in source but not in target - it's an insert
            Result.Append;
            for I := 0 to Source.FieldCount - 1 do
              Result.Fields[I].Value := Source.Fields[I].Value;
            Result.FieldByName('ChangeType').AsString := 'Insert';
            Result.Post;
          end;
          
          Source.Next;
        end;
        
        // Third pass: find deletions (records in target but not in source)
        Target.First;
        while not Target.Eof do
        begin
          // Build composite key value
          TargetKeyValue := '';
          for I := 0 to KeyFieldList.Count - 1 do
          begin
            if I > 0 then
              TargetKeyValue := TargetKeyValue + '|';
            TargetKeyValue := TargetKeyValue + Target.FieldByName(KeyFieldList[I]).AsString;
          end;
          
          if not SourceHashMap.ContainsKey(TargetKeyValue) then
          begin
            // Record exists in target but not in source - it's a delete
            Result.Append;
            for I := 0 to Target.FieldCount - 1 do
              Result.Fields[I].Value := Target.Fields[I].Value;
            Result.FieldByName('ChangeType').AsString := 'Delete';
            Result.Post;
          end;
          
          Target.Next;
        end;
      finally
        if Source.BookmarkValid(SourceBookmark) then
          Source.Bookmark := SourceBookmark;
        if Target.BookmarkValid(TargetBookmark) then
          Target.Bookmark := TargetBookmark;
      end;
    finally
      Source.EnableControls;
      Target.EnableControls;
    end;
  finally
    KeyFieldList.Free;
    SourceHashMap.Free;
    TargetHashMap.Free;
  end;
end;

class function TDFDiffEngine.ExtractChangedRecords(Source, Target: TDataSet;
  const KeyFields: string): TFDMemTable;
begin
  Result := CompareDatasets(Source, Target, KeyFields);
end;

end.