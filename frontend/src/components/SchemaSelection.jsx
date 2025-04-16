import React, { useState, useEffect } from 'react';
import {
  Box,
  Paper,
  Typography,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Checkbox,
  ListItemText,
  OutlinedInput,
  Button,
  Alert,
  CircularProgress,
  TextField,
  Grid
} from '@mui/material';
import axios from 'axios';

const SchemaSelection = ({ config, onUpdate, onNext, onBack }) => {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [tables, setTables] = useState([]);
  const [columns, setColumns] = useState([]);
  const [joinEnabled, setJoinEnabled] = useState(false);

  useEffect(() => {
    if (config.sourceType === 'CLICKHOUSE') {
      loadTables();
    } else if (config.flatFilePath) {
      loadFileSchema();
    }
  }, []);

  const loadTables = async () => {
    setLoading(true);
    setError('');
    try {
      const response = await axios.post('http://localhost:8080/api/ingestion/tables', config.clickHouseConfig);
      setTables(response.data);
    } catch (err) {
      setError(err.response?.data?.error || 'Failed to load tables');
    } finally {
      setLoading(false);
    }
  };

  const loadFileSchema = async () => {
    setLoading(true);
    setError('');
    try {
      const response = await axios.post(
        'http://localhost:8080/api/ingestion/file-schema',
        null,
        {
          params: {
            filePath: config.flatFilePath,
            delimiter: config.delimiter
          }
        }
      );
      setColumns(response.data);
    } catch (err) {
      setError(err.response?.data?.error || 'Failed to load file schema');
    } finally {
      setLoading(false);
    }
  };

  const loadColumns = async (tableName) => {
    setLoading(true);
    setError('');
    try {
      const response = await axios.post(
        'http://localhost:8080/api/ingestion/columns',
        config.clickHouseConfig,
        { params: { tableName } }
      );
      setColumns(response.data);
    } catch (err) {
      setError(err.response?.data?.error || 'Failed to load columns');
    } finally {
      setLoading(false);
    }
  };

  const handleTableChange = (event) => {
    const selectedTables = event.target.value;
    onUpdate({ selectedTables });
    if (selectedTables.length === 1) {
      loadColumns(selectedTables[0]);
    }
    setJoinEnabled(selectedTables.length > 1);
  };

  const handleColumnChange = (event) => {
    onUpdate({ selectedColumns: event.target.value });
  };

  const handleJoinConditionChange = (event) => {
    onUpdate({ joinCondition: event.target.value });
  };

  const handleNext = () => {
    if (!config.selectedColumns.length) {
      setError('Please select at least one column');
      return;
    }
    if (joinEnabled && !config.joinCondition) {
      setError('Please specify join condition');
      return;
    }
    onNext();
  };

  return (
    <Box sx={{ py: 3 }}>
      <Typography variant="h5" gutterBottom>
        Select Schema
      </Typography>

      <Paper sx={{ p: 3, mt: 2 }}>
        {config.sourceType === 'CLICKHOUSE' && (
          <FormControl fullWidth sx={{ mb: 2 }}>
            <InputLabel>Select Tables</InputLabel>
            <Select
              multiple
              value={config.selectedTables}
              onChange={handleTableChange}
              input={<OutlinedInput label="Select Tables" />}
              renderValue={(selected) => selected.join(', ')}
            >
              {tables.map((table) => (
                <MenuItem key={table} value={table}>
                  <Checkbox checked={config.selectedTables.indexOf(table) > -1} />
                  <ListItemText primary={table} />
                </MenuItem>
              ))}
            </Select>
          </FormControl>
        )}

        {joinEnabled && (
          <TextField
            fullWidth
            label="Join Condition"
            placeholder="e.g. table1.id = table2.id"
            value={config.joinCondition}
            onChange={handleJoinConditionChange}
            sx={{ mb: 2 }}
          />
        )}

        <FormControl fullWidth>
          <InputLabel>Select Columns</InputLabel>
          <Select
            multiple
            value={config.selectedColumns}
            onChange={handleColumnChange}
            input={<OutlinedInput label="Select Columns" />}
            renderValue={(selected) => selected.join(', ')}
          >
            {columns.map((column) => (
              <MenuItem key={column} value={column}>
                <Checkbox checked={config.selectedColumns.indexOf(column) > -1} />
                <ListItemText primary={column} />
              </MenuItem>
            ))}
          </Select>
        </FormControl>

        {error && (
          <Alert severity="error" sx={{ mt: 2 }}>
            {error}
          </Alert>
        )}

        {loading && (
          <Box sx={{ display: 'flex', justifyContent: 'center', mt: 2 }}>
            <CircularProgress />
          </Box>
        )}
      </Paper>

      <Box sx={{ mt: 3, display: 'flex', justifyContent: 'space-between' }}>
        <Button onClick={onBack}>Back</Button>
        <Button
          variant="contained"
          onClick={handleNext}
          disabled={loading}
        >
          Next
        </Button>
      </Box>
    </Box>
  );
};

export default SchemaSelection;