import React from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  Grid,
  Button,
  CardActionArea
} from '@mui/material';
import StorageIcon from '@mui/icons-material/Storage';
import InsertDriveFileIcon from '@mui/icons-material/InsertDriveFile';

const SourceSelection = ({ config, onUpdate, onNext }) => {
  const handleSourceSelect = (sourceType) => {
    onUpdate({ sourceType });
    onNext();
  };

  return (
    <Box sx={{ py: 3 }}>
      <Typography variant="h5" gutterBottom>
        Select Data Source
      </Typography>
      <Grid container spacing={3} sx={{ mt: 2 }}>
        <Grid item xs={12} md={6}>
          <Card
            sx={{
              height: '100%',
              cursor: 'pointer',
              transition: '0.3s',
              '&:hover': {
                transform: 'translateY(-5px)',
                boxShadow: 3
              }
            }}
          >
            <CardActionArea
              onClick={() => handleSourceSelect('CLICKHOUSE')}
              sx={{ height: '100%' }}
            >
              <CardContent sx={{ textAlign: 'center', py: 4 }}>
                <StorageIcon sx={{ fontSize: 60, color: 'primary.main', mb: 2 }} />
                <Typography variant="h6" gutterBottom>
                  ClickHouse Database
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Connect to a ClickHouse database using JWT authentication
                </Typography>
              </CardContent>
            </CardActionArea>
          </Card>
        </Grid>

        <Grid item xs={12} md={6}>
          <Card
            sx={{
              height: '100%',
              cursor: 'pointer',
              transition: '0.3s',
              '&:hover': {
                transform: 'translateY(-5px)',
                boxShadow: 3
              }
            }}
          >
            <CardActionArea
              onClick={() => handleSourceSelect('FLAT_FILE')}
              sx={{ height: '100%' }}
            >
              <CardContent sx={{ textAlign: 'center', py: 4 }}>
                <InsertDriveFileIcon sx={{ fontSize: 60, color: 'primary.main', mb: 2 }} />
                <Typography variant="h6" gutterBottom>
                  Flat File
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Import data from or export data to a flat file (CSV)
                </Typography>
              </CardContent>
            </CardActionArea>
          </Card>
        </Grid>
      </Grid>
    </Box>
  );
};

export default SourceSelection;