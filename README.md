# DFDataSync

> Data Synchronization and Replication Framework for Modern Delphi Applications

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Overview

DFDataSync is a comprehensive Delphi library designed to handle robust data synchronization and replication between local databases, remote servers, and offline clients. It provides a flexible framework for implementing data synchronization with conflict resolution, change tracking, and efficient differential synchronization.

## Features

- **Bidirectional Synchronization**: Support for uploading, downloading, or bidirectional sync operations
- **Intelligent Change Tracking**: Track changes to your data with detailed metadata
- **Robust Conflict Resolution**: Multiple strategies for resolving data conflicts
- **Differential Synchronization**: Only transmit changed data for efficient network usage
- **Event-Driven Architecture**: Full notification system for sync progress and events
- **Background Processing**: Non-blocking sync operations via Delphi's task parallel library
- **Extensible Design**: Easy to customize and extend for specific business needs
- **Automatic Scheduling**: Built-in timer for periodic synchronization
- **FireDAC Integration**: Seamless integration with FireDAC components

## Requirements

- Delphi 10.3 or higher
- FireDAC components

## Installation

1. Clone the repository or download the latest release
2. Add the source directory to your project's search path
3. Add `DFDataSync` to your uses clause

## Quick Start Guide

### Basic Setup

```pascal
uses
  DFDataSync;

var
  SourceDB, TargetDB: TFDConnection;
  SourceEndpoint, TargetEndpoint: IDFSyncEndpoint;
  SyncController: TDFSyncController;
  
begin
  // Setup connections
  SourceDB := TFDConnection.Create(nil);
  SourceDB.ConnectionDefName := 'LocalDB';
  SourceDB.Connected := True;
  
  TargetDB := TFDConnection.Create(nil);
  TargetDB.ConnectionDefName := 'RemoteDB';
  TargetDB.Connected := True;
  
  // Create endpoints
  SourceEndpoint := TDFLocalDatabaseEndpoint.Create(SourceDB);
  TargetEndpoint := TDFLocalDatabaseEndpoint.Create(TargetDB);
  
  // Add entities to synchronize
  (SourceEndpoint as TDFLocalDatabaseEndpoint).AddEntity('Customers', 'Customers');
  (SourceEndpoint as TDFLocalDatabaseEndpoint).AddEntity('Orders', 'Orders');
  (TargetEndpoint as TDFLocalDatabaseEndpoint).AddEntity('Customers', 'Customers');
  (TargetEndpoint as TDFLocalDatabaseEndpoint).AddEntity('Orders', 'Orders');
  
  // Create and configure sync controller
  SyncController := TDFSyncController.Create(nil);
  SyncController.SetEndpoints(SourceEndpoint, TargetEndpoint);
  SyncController.SyncDirection := sdBidirectional;
  SyncController.ConflictResolution := crNewerWins;
  
  // Set up event handlers
  SyncController.OnConflict := HandleConflict;
  SyncController.OnProgress := ShowSyncProgress;
  SyncController.OnSyncComplete := SyncCompleted;
  
  // Start synchronization
  SyncController.StartSync;
end;
```

### Change Tracking Tables

The framework automatically creates and manages these tables:

- **DFChangeLog**: Records all data changes with metadata
- **DFSyncState**: Tracks synchronization state across devices

## Advanced Usage

### Custom Conflict Resolution

```pascal
procedure TMyForm.HandleConflict(Sender: TObject; const EntityName: string;
  const PrimaryKey: Variant; var Resolution: TDFConflictResolution);
begin
  // Show conflict dialog to user
  if MessageDlg('Conflict detected for ' + EntityName + '. Use local version?',
    mtConfirmation, [mbYes, mbNo], 0) = mrYes then
    Resolution := crSourceWins
  else
    Resolution := crTargetWins;
end;
```

### Periodic Synchronization

```pascal
// Set up automatic sync every 5 minutes
SyncController.SyncInterval := 5 * 60;
```

### Progress Feedback

```pascal
procedure TMyForm.ShowSyncProgress(Sender: TObject; const CurrentItem, TotalItems: Integer;
  const EntityName, OperationType: string);
begin
  ProgressBar1.Position := Round((CurrentItem / TotalItems) * 100);
  StatusLabel.Caption := Format('Synchronizing %s (%s): %d of %d', 
    [EntityName, OperationType, CurrentItem, TotalItems]);
end;
```

## API Reference

### Key Classes

- **TDFSyncController**: Main class to coordinate synchronization operations
- **TDFDBChangeTracker**: Tracks data changes in a database
- **TDFLocalDatabaseEndpoint**: Represents a database endpoint for synchronization
- **TDFDiffEngine**: Utility class for finding differences between datasets

### Key Interfaces

- **IDFChangeTracker**: Interface for tracking data changes
- **IDFSyncEndpoint**: Interface for synchronization endpoints

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributions

Contributions are welcome! Please feel free to submit a Pull Request.

## Credits

- Developed by Delphifan
- (c) 2025
