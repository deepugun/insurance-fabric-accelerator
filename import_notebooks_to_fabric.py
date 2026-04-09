#!/usr/bin/env python3
"""
Import Notebooks to Fabric via REST API
Programmatically upload all 15 notebooks to a Fabric workspace
"""

import os
import json
import base64
import requests
import subprocess
from pathlib import Path
from typing import Optional, Dict, List

class FabricNotebookImporter:
    """Import notebooks to Microsoft Fabric workspace via REST API."""
    
    def __init__(self, workspace_id: str, access_token: Optional[str] = None):
        """
        Initialize the importer.
        
        Args:
            workspace_id: Fabric workspace ID (GUID)
            access_token: Bearer token for authentication (if None, uses Azure CLI)
        """
        self.workspace_id = workspace_id
        self.access_token = access_token or self._get_azure_cli_token()
        self.api_base = "https://api.fabric.microsoft.com/v1"
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        
    def _get_azure_cli_token(self) -> str:
        """Get access token from Azure CLI."""
        print("🔑 Getting access token from Azure CLI...")
        try:
            result = subprocess.run(
                ["az", "account", "get-access-token", "--resource", "https://api.fabric.microsoft.com"],
                capture_output=True,
                text=True,
                check=True
            )
            token_data = json.loads(result.stdout)
            print("✅ Successfully obtained access token")
            return token_data["accessToken"]
        except subprocess.CalledProcessError as e:
            print("❌ Failed to get Azure CLI token. Please run: az login")
            raise
        except FileNotFoundError:
            print("❌ Azure CLI not found. Please install: https://aka.ms/install-azure-cli")
            raise
    
    def verify_workspace(self) -> bool:
        """Verify workspace exists and is accessible."""
        print(f"\n🔍 Verifying workspace: {self.workspace_id}")
        url = f"{self.api_base}/workspaces/{self.workspace_id}"
        
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            workspace = response.json()
            print(f"✅ Workspace found: {workspace.get('displayName', 'Unknown')}")
            return True
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                print(f"❌ Workspace not found: {self.workspace_id}")
            elif e.response.status_code == 403:
                print(f"❌ Access denied to workspace: {self.workspace_id}")
            else:
                print(f"❌ Error verifying workspace: {e}")
            return False
    
    def list_existing_notebooks(self) -> List[Dict]:
        """List existing notebooks in workspace."""
        print(f"\n📋 Listing existing notebooks in workspace...")
        url = f"{self.api_base}/workspaces/{self.workspace_id}/notebooks"
        
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            notebooks = response.json().get("value", [])
            print(f"✅ Found {len(notebooks)} existing notebooks")
            return notebooks
        except requests.exceptions.HTTPError as e:
            print(f"❌ Error listing notebooks: {e}")
            return []
    
    def import_notebook(self, notebook_path: Path, display_name: str, overwrite: bool = False) -> Optional[str]:
        """
        Import a single notebook file to Fabric.
        
        Args:
            notebook_path: Path to .ipynb file
            display_name: Display name for the notebook in Fabric
            overwrite: Whether to overwrite if notebook exists
            
        Returns:
            Notebook ID if successful, None otherwise
        """
        print(f"\n📤 Importing: {display_name}")
        
        # Read notebook content
        try:
            with open(notebook_path, 'r', encoding='utf-8') as f:
                notebook_content = f.read()
        except Exception as e:
            print(f"   ❌ Failed to read file: {e}")
            return None
        
        # Check if notebook already exists
        existing_notebooks = self.list_existing_notebooks()
        existing = next((nb for nb in existing_notebooks if nb.get('displayName') == display_name), None)
        
        if existing and not overwrite:
            print(f"   ⚠️  Notebook already exists: {display_name}")
            print(f"   💡 Use overwrite=True to replace it")
            return existing.get('id')
        
        # Prepare notebook definition
        # Encode content as base64
        content_base64 = base64.b64encode(notebook_content.encode('utf-8')).decode('utf-8')
        
        payload = {
            "displayName": display_name,
            "definition": {
                "format": "ipynb",
                "parts": [
                    {
                        "path": "notebook-content.py",
                        "payload": content_base64,
                        "payloadType": "InlineBase64"
                    }
                ]
            }
        }
        
        # Create or update notebook
        if existing and overwrite:
            # Update existing notebook
            url = f"{self.api_base}/workspaces/{self.workspace_id}/notebooks/{existing['id']}"
            method = requests.patch
            print(f"   🔄 Updating existing notebook...")
        else:
            # Create new notebook
            url = f"{self.api_base}/workspaces/{self.workspace_id}/notebooks"
            method = requests.post
            print(f"   ➕ Creating new notebook...")
        
        try:
            response = method(url, headers=self.headers, json=payload)
            response.raise_for_status()
            
            if response.status_code in (200, 201):
                notebook_id = response.json().get('id')
                print(f"   ✅ Success! Notebook ID: {notebook_id}")
                return notebook_id
            else:
                print(f"   ⚠️  Unexpected status: {response.status_code}")
                return None
                
        except requests.exceptions.HTTPError as e:
            print(f"   ❌ Import failed: {e}")
            if e.response:
                print(f"   Response: {e.response.text}")
            return None
    
    def import_all_notebooks(self, notebooks_dir: Path, overwrite: bool = False) -> Dict[str, str]:
        """
        Import all notebooks from a directory.
        
        Args:
            notebooks_dir: Directory containing .ipynb files
            overwrite: Whether to overwrite existing notebooks
            
        Returns:
            Dictionary mapping notebook names to IDs
        """
        print("\n" + "="*70)
        print("🚀 IMPORTING NOTEBOOKS TO FABRIC")
        print("="*70)
        
        # Find all notebook files
        notebook_files = sorted(notebooks_dir.glob("*.ipynb"))
        
        if not notebook_files:
            print(f"❌ No notebook files found in: {notebooks_dir}")
            return {}
        
        print(f"\n📦 Found {len(notebook_files)} notebooks to import")
        
        # Import each notebook
        results = {}
        success_count = 0
        skip_count = 0
        fail_count = 0
        
        for notebook_file in notebook_files:
            # Use filename without extension as display name
            display_name = notebook_file.stem
            
            notebook_id = self.import_notebook(notebook_file, display_name, overwrite)
            
            if notebook_id:
                results[display_name] = notebook_id
                if overwrite:
                    success_count += 1
                else:
                    # Could be existing or new
                    success_count += 1
            else:
                fail_count += 1
        
        # Summary
        print("\n" + "="*70)
        print("📊 IMPORT SUMMARY")
        print("="*70)
        print(f"✅ Successful: {success_count}")
        print(f"❌ Failed: {fail_count}")
        print(f"📦 Total: {len(notebook_files)}")
        print("\n💡 Next Steps:")
        print("   1. Open Fabric workspace in browser")
        print("   2. Create lakehouses: insurance_bronze, insurance_silver, insurance_gold, insurance_metadata")
        print("   3. Attach lakehouses to notebooks")
        print("   4. Run 01_demo_data_generator.ipynb to create sample data")
        print("="*70 + "\n")
        
        return results


def main():
    """Main execution function."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Import Insurance Fabric Accelerator notebooks to Fabric workspace"
    )
    parser.add_argument(
        "--workspace-id",
        required=True,
        help="Fabric workspace ID (GUID)"
    )
    parser.add_argument(
        "--notebooks-dir",
        default="notebooks",
        help="Directory containing notebook files (default: notebooks)"
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing notebooks"
    )
    parser.add_argument(
        "--token",
        help="Bearer access token (if not provided, uses Azure CLI)"
    )
    
    args = parser.parse_args()
    
    # Resolve notebooks directory
    notebooks_dir = Path(args.notebooks_dir)
    if not notebooks_dir.is_absolute():
        notebooks_dir = Path(__file__).parent / notebooks_dir
    
    if not notebooks_dir.exists():
        print(f"❌ Notebooks directory not found: {notebooks_dir}")
        return 1
    
    print("\n" + "="*70)
    print("Insurance Fabric Accelerator - Notebook Importer")
    print("="*70)
    print(f"Workspace ID: {args.workspace_id}")
    print(f"Notebooks Directory: {notebooks_dir}")
    print(f"Overwrite: {args.overwrite}")
    print("="*70)
    
    # Create importer
    try:
        importer = FabricNotebookImporter(
            workspace_id=args.workspace_id,
            access_token=args.token
        )
    except Exception as e:
        print(f"❌ Failed to initialize importer: {e}")
        return 1
    
    # Verify workspace
    if not importer.verify_workspace():
        return 1
    
    # Import notebooks
    results = importer.import_all_notebooks(notebooks_dir, overwrite=args.overwrite)
    
    if not results:
        print("❌ No notebooks were imported")
        return 1
    
    print(f"\n✅ Successfully processed {len(results)} notebooks")
    return 0


if __name__ == "__main__":
    exit(main())
