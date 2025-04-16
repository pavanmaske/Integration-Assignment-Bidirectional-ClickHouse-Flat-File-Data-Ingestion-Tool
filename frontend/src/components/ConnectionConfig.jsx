import React, { useState } from 'react';
import {
  Box,
  TextField,
  Button,
  Typography,
  Paper,
  Alert,
  CircularProgress,
  Grid
} from '@mui/material';
import axios from 'axios';

const ConnectionConfig = ({ config, onUpdate, onNext, onBack }) => {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  const [selectedFile, setSelectedFile] = useState(null);

  const handleClickHouseConfigChange = (field) => (event) => {
    onUpdate({
      clickHouseConfig: {
        ...config.clickHouseConfig,
        [field]: event.target.value
      }
    });
  };

  const handleFileSelect = (event) => {
    setSelectedFile(event.target.files[0]);
  };

  const handleTestConnection = async () => {
    setLoading(true);
    setError('');
    setSuccess('');

    try {
      const response = await axios.post('http://localhost:8080/api/ingestion/test-connection', config.clickHouseConfig);
      setSuccess('Connection successful!');
    } catch (err) {
      setError(err.response?.data?.error || 'Connection failed');
    } finally {
      setLoading(false);
    }
  };

  const handleFileUpload = async () => {
    if (!selectedFile) {
      setError('Please select a file first');
      return;
    }

    setLoading(true);
    setError('');

    const formData = new FormData();
    formData.append('file', selectedFile);

    try {
      const response = await axios.post('http://localhost:8080/api/ingestion/upload', formData);
      onUpdate({
        flatFilePath: response.data.filePath,
        delimiter: ','
      });
      setSuccess('File uploaded successfully!');
    } catch (err) {
      setError(err.response?.data?.error || 'File upload failed');
    } finally {
      setLoading(false);
    }
  };

  const handleNext = () => {
    if (config.sourceType === 'CLICKHOUSE' && !success) {
      setError('Please test the connection first');
      return;
    }
    if (config.sourceType === 'FLAT_FILE' && !config.flatFilePath) {
      setError('Please upload a file first');
      return;
    }
    onNext();
  };

  return (
    <Box sx={{ py: 3 }}>
      <Typography variant="h5" gutterBottom>
        Configure Connection
      </Typography>

      <Paper sx={{ p: 3, mt: 2 }}>
        {config.sourceType === 'CLICKHOUSE' ? (
          <Grid container spacing={2}>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Host"
                value={config.clickHouseConfig.host}
                onChange={handleClickHouseConfigChange('host')}
                margin="normal"
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Port"
                type="number"
                value={config.clickHouseConfig.port}
                onChange={handleClickHouseConfigChange('port')}
                margin="normal"
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Database"
                value={config.clickHouseConfig.database}
                onChange={handleClickHouseConfigChange('database')}
                margin="normal"
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Username"
                value={config.clickHouseConfig.username}
                onChange={handleClickHouseConfigChange('username')}
                margin="normal"
              />
            </Grid>
            <Grid item xs={12}>
              <TextField
                fullWidth
                label="JWT Token"
                value={config.clickHouseConfig.jwtToken}
                onChange={handleClickHouseConfigChange('jwtToken')}
                margin="normal"
              />
            </Grid>
            <Grid item xs={12}>
              <Button
                variant="contained"
                onClick={handleTestConnection}
                disabled={loading}
                sx={{ mr: 2 }}
              >
                {loading ? <CircularProgress size={24} /> : 'Test Connection'}
              </Button>
            </Grid>
          </Grid>
        ) : (
          <Box>
            <input
              accept=".csv,.txt"
              style={{ display: 'none' }}
              id="file-input"
              type="file"
              onChange={handleFileSelect}
            />
            <label htmlFor="file-input">
              <Button variant="contained" component="span" sx={{ mr: 2 }}>
                Choose File
              </Button>
            </label>
            {selectedFile && (
              <Typography variant="body2" sx={{ mt: 1 }}>
                Selected file: {selectedFile.name}
              </Typography>
            )}
            <Button
              variant="contained"
              onClick={handleFileUpload}
              disabled={!selectedFile || loading}
              sx={{ mt: 2 }}
            >
              {loading ? <CircularProgress size={24} /> : 'Upload'}
            </Button>
          </Box>
        )}

        {error && (
          <Alert severity="error" sx={{ mt: 2 }}>
            {error}
          </Alert>
        )}
        {success && (
          <Alert severity="success" sx={{ mt: 2 }}>
            {success}
          </Alert>
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

export default ConnectionConfig;